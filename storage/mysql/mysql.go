// Copyright 2024 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package mysql contains a MySQL-based storage implementation for Tessera.
package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/internal/migrate"
	storage "github.com/transparency-dev/tessera/storage/internal"
	"k8s.io/klog/v2"
)

const (
	selectCompatibilityVersionSQL    = "SELECT `compatibilityVersion` FROM `Tessera` WHERE `id` = 0"
	selectCheckpointByIDSQL          = "SELECT `note`, `published_at` FROM `Checkpoint` WHERE `id` = ?"
	selectCheckpointByIDForUpdateSQL = selectCheckpointByIDSQL + " FOR UPDATE"
	replaceCheckpointSQL             = "REPLACE INTO `Checkpoint` (`id`, `note`, `published_at`) VALUES (?, ?, ?)"
	selectTreeStateByIDSQL           = "SELECT `size`, `root` FROM `TreeState` WHERE `id` = ?"
	selectTreeStateByIDForUpdateSQL  = selectTreeStateByIDSQL + " FOR UPDATE"
	replaceTreeStateSQL              = "REPLACE INTO `TreeState` (`id`, `size`, `root`) VALUES (?, ?, ?)"
	selectSubtreeByLevelAndIndexSQL  = "SELECT `nodes` FROM `Subtree` WHERE `level` = ? AND `index` = ?"
	replaceSubtreeSQL                = "REPLACE INTO `Subtree` (`level`, `index`, `nodes`) VALUES (?, ?, ?)"
	selectTiledLeavesSQL             = "SELECT `size`, `data` FROM `TiledLeaves` WHERE `tile_index` = ?"
	streamTiledLeavesSQL             = "SELECT `tile_index`, `size`, `data` FROM `TiledLeaves` WHERE `tile_index` >= ? ORDER BY `tile_index` ASC"
	replaceTiledLeavesSQL            = "REPLACE INTO `TiledLeaves` (`tile_index`, `size`, `data`) VALUES (?, ?, ?)"

	checkpointID = 0
	treeStateID  = 0

	schemaCompatibilityVersion = 1

	minCheckpointInterval = time.Second
)

// Storage is a MySQL-based storage implementation for Tessera.
type Storage struct {
	db *sql.DB
}

// New creates a new instance of the MySQL-based Storage.
func New(ctx context.Context, db *sql.DB) (*Storage, error) {
	s := &Storage{
		db: db,
	}
	if err := s.db.Ping(); err != nil {
		klog.Errorf("Failed to ping database: %v", err)
		return nil, err
	}
	if err := s.ensureVersion(ctx, schemaCompatibilityVersion); err != nil {
		return nil, fmt.Errorf("incompatible schema version: %v", err)
	}
	return s, nil
}

// Note that `tessera.WithCheckpointSigner()` is mandatory in the `opts` argument.
func (s *Storage) Appender(ctx context.Context, opts *tessera.AppendOptions) (*tessera.Appender, tessera.LogReader, error) {
	if opts.CheckpointInterval() < minCheckpointInterval {
		return nil, nil, fmt.Errorf("requested CheckpointInterval too low - %v < %v", opts.CheckpointInterval(), minCheckpointInterval)
	}

	a := &appender{
		s:             s,
		newCheckpoint: opts.CheckpointPublisher(s, http.DefaultClient),
		cpUpdated:     make(chan struct{}, 1),
	}
	a.queue = storage.NewQueue(ctx, opts.BatchMaxAge(), opts.BatchMaxSize(), a.sequenceBatch)

	if err := s.maybeInitTree(ctx); err != nil {
		return nil, nil, fmt.Errorf("maybeInitTree: %v", err)
	}
	a.cpUpdated <- struct{}{}

	go func(ctx context.Context, i time.Duration) {
		t := time.NewTicker(i)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-a.cpUpdated:
			case <-t.C:
			}
			if err := a.publishCheckpoint(ctx, i); err != nil {
				klog.Warningf("publishCheckpoint: %v", err)
			}
		}
	}(ctx, opts.CheckpointInterval())

	return &tessera.Appender{
		Add: a.Add,
	}, s, nil
}

func (s *Storage) ensureVersion(ctx context.Context, wantVersion uint8) error {
	row := s.db.QueryRowContext(ctx, selectCompatibilityVersionSQL)
	if row.Err() != nil {
		return row.Err()
	}
	var gotVersion uint8
	if err := row.Scan(&gotVersion); err != nil {
		return fmt.Errorf("failed to read Tessera version from DB: %v", err)
	}
	if gotVersion != wantVersion {
		return fmt.Errorf("DB has Tessera compatibility version of %d, but version %d required", gotVersion, wantVersion)
	}
	return nil
}

// maybeInitTree will insert an initial "empty tree" row into the
// TreeState table iff no row already exists.
//
// This method doesn't also publish this new empty tree as a Checkpoint,
// rather, such a checkpoint will be published asynchronously by the
// same mechanism used to publish future checkpoints. Although in _this_
// case it would be expected to happen in very short order given that it's
// likely that no row currently exists in the Checkpoints table either.
func (s *Storage) maybeInitTree(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("being tx init tree state: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			klog.Errorf("Failed to rollback in write initial tree state: %v", err)
		}
	}()

	treeState, err := s.readTreeStateForUpdate(ctx, tx)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		klog.Errorf("Failed to read tree state: %v", err)
		return err
	}
	if treeState == nil {
		klog.Infof("Initializing tree state")
		if err := s.writeTreeState(ctx, tx, 0, rfc6962.DefaultHasher.EmptyRoot()); err != nil {
			klog.Errorf("Failed to write initial tree state: %v", err)
			return err
		}
		// Only need to commit if we've actually initialised the tree state, otherwise we'll
		// rely on the defer'd rollback to tidy up.
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit init tree state: %v", err)
		}
	}
	return nil
}

// ReadCheckpoint returns the latest stored checkpoint.
// If the checkpoint is not found, it returns os.ErrNotExist.
func (s *Storage) ReadCheckpoint(ctx context.Context) ([]byte, error) {
	row := s.db.QueryRowContext(ctx, selectCheckpointByIDSQL, checkpointID)
	if err := row.Err(); err != nil {
		return nil, err
	}

	var checkpoint []byte
	var at int64
	if err := row.Scan(&checkpoint, &at); err != nil {
		if err == sql.ErrNoRows {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("scan checkpoint: %v", err)
	}
	return checkpoint, nil
}

type treeState struct {
	size uint64
	root []byte
}

// readTreeState returns the currently stored state information.
// If there is no stored tree state, it returns os.ErrNotExist.
func (s *Storage) readTreeState(ctx context.Context) (*treeState, error) {
	row := s.db.QueryRowContext(ctx, selectTreeStateByIDSQL, treeStateID)
	if err := row.Err(); err != nil {
		return nil, err
	}

	r := &treeState{}
	if err := row.Scan(&r.size, &r.root); err != nil {
		if err == sql.ErrNoRows {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("scan tree state: %v", err)
	}
	return r, nil
}

// readTreeStateForUpdate returns the currently stored tree state information, and locks the row for update using the provided transaction.
// If there is no stored tree state, it returns os.ErrNotExist.
func (s *Storage) readTreeStateForUpdate(ctx context.Context, tx *sql.Tx) (*treeState, error) {
	row := tx.QueryRowContext(ctx, selectTreeStateByIDForUpdateSQL, treeStateID)
	if err := row.Err(); err != nil {
		return nil, err
	}

	r := &treeState{}
	if err := row.Scan(&r.size, &r.root); err != nil {
		if err == sql.ErrNoRows {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("scan tree state: %v", err)
	}
	return r, nil
}

// writeTreeState updates the TreeState table with the new tree state information.
func (s *Storage) writeTreeState(ctx context.Context, tx *sql.Tx, size uint64, rootHash []byte) error {
	if _, err := tx.ExecContext(ctx, replaceTreeStateSQL, treeStateID, size, rootHash); err != nil {
		klog.Errorf("Failed to execute replaceTreeStateSQL: %v", err)
		return err
	}

	return nil
}

// ReadTile returns a full tile or a partial tile at the given level, index and treeSize.
// If the tile is not found, it returns os.ErrNotExist.
//
// Note that if a partial tile is requested, but a larger tile is available, this
// will return the largest tile available. This could be trimmed to return only the
// number of entries specifically requested if this behaviour becomes problematic.
func (s *Storage) ReadTile(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
	row := s.db.QueryRowContext(ctx, selectSubtreeByLevelAndIndexSQL, level, index)
	if err := row.Err(); err != nil {
		return nil, err
	}

	var tile []byte
	if err := row.Scan(&tile); err != nil {
		if err == sql.ErrNoRows {
			return nil, os.ErrNotExist
		}

		return nil, fmt.Errorf("scan tile: %v", err)
	}

	numEntries := uint64(len(tile) / sha256.Size)
	requestedEntries := uint64(p)
	if requestedEntries == 0 {
		requestedEntries = layout.TileWidth
	}
	if requestedEntries > numEntries {
		// If the user has requested a size larger than we have, they can't have it
		return nil, os.ErrNotExist
	}

	return tile, nil
}

// writeTile replaces the tile nodes at the given level and index.
func (s *Storage) writeTile(ctx context.Context, tx *sql.Tx, level, index uint64, nodes []byte) error {
	if _, err := tx.ExecContext(ctx, replaceSubtreeSQL, level, index, nodes); err != nil {
		klog.Errorf("Failed to execute replaceSubtreeSQL: %v", err)
		return err
	}

	return nil
}

// ReadEntryBundle returns the log entries at the given index.
// If the entry bundle is not found, it returns os.ErrNotExist.
//
// Note that if a partial tile is requested, but a larger tile is available, this
// will return the largest tile available. This could be trimmed to return only the
// number of entries specifically requested if this behaviour becomes problematic.
func (s *Storage) ReadEntryBundle(ctx context.Context, index uint64, p uint8) ([]byte, error) {
	row := s.db.QueryRowContext(ctx, selectTiledLeavesSQL, index)
	if err := row.Err(); err != nil {
		return nil, err
	}

	var size uint32
	var entryBundle []byte
	if err := row.Scan(&size, &entryBundle); err != nil {
		if err == sql.ErrNoRows {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("scan entry bundle: %v", err)
	}

	requestedSize := uint32(p)
	if requestedSize == 0 {
		requestedSize = layout.EntryBundleWidth
	}

	if requestedSize > size {
		return nil, fmt.Errorf("bundle with %d entries requested, but only %d available: %w", requestedSize, size, os.ErrNotExist)
	}

	return entryBundle, nil
}

// IntegratedSize returns the current size of the integrated tree.
//
// This is part of the tessera LogReader contract.
func (s *Storage) IntegratedSize(ctx context.Context) (uint64, error) {
	ts, err := s.readTreeState(ctx)
	if err != nil {
		return 0, fmt.Errorf("readTreeState: %v", err)
	}
	return ts.size, nil
}

// NextIndex returns the next available leaf index.
//
// Currently, this is the same as the integrated size since new leaves are integrated synchronously.
// This is part of the tessera LogReader contract.
func (s *Storage) NextIndex(ctx context.Context) (uint64, error) {
	return s.IntegratedSize(ctx)
}

// dbExecContext describes something which can support the sql ExecContext function.
// this allows us to use either sql.Tx or sql.DB.
type dbExecContext interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *Storage) writeEntryBundle(ctx context.Context, tx dbExecContext, index uint64, size uint32, entryBundle []byte) error {
	if _, err := tx.ExecContext(ctx, replaceTiledLeavesSQL, index, size, entryBundle); err != nil {
		klog.Errorf("Failed to execute replaceTiledLeavesSQL: %v", err)
		return err
	}

	return nil
}

// appender implements the tessera Append lifecycle.
type appender struct {
	s             *Storage
	queue         *storage.Queue
	newCheckpoint func(context.Context, uint64, []byte) ([]byte, error)
	cpUpdated     chan struct{}
}

// publishCheckpoint creates a new checkpoint for the given size and root hash, and stores it in the
// Checkpoint table.
func (a *appender) publishCheckpoint(ctx context.Context, interval time.Duration) error {
	tx, err := a.s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			klog.Warningf("publishCheckpoint rollback failed: %v", err)
		}
	}()

	var note string
	var at int64
	if err := tx.QueryRowContext(ctx, selectCheckpointByIDForUpdateSQL, checkpointID).Scan(&note, &at); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("scan checkpoint: %v", err)
	}
	if time.Since(time.UnixMilli(at)) < interval {
		// Too soon, try again later.
		klog.V(1).Info("skipping publish - too soon")
		return nil
	}

	treeState, err := a.s.readTreeStateForUpdate(ctx, tx)
	if err != nil {
		return fmt.Errorf("readTreeState: %v", err)
	}

	rawCheckpoint, err := a.newCheckpoint(ctx, treeState.size, treeState.root)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, replaceCheckpointSQL, checkpointID, rawCheckpoint, time.Now().UnixMilli()); err != nil {
		return err
	}

	klog.V(2).Infof("Published latest checkpoint: %d, %x", treeState.size, treeState.root)

	return tx.Commit()
}

// Add is the entrypoint for adding entries to a sequencing log.
func (a *appender) Add(ctx context.Context, entry *tessera.Entry) tessera.IndexFuture {
	return a.queue.Add(ctx, entry)
}

// sequenceBatch writes the entries from the provided batch into the entry bundle files of the log.
//
// This func starts filling entries bundles at the next available slot in the log, ensuring that the
// sequenced entries are contiguous from the zeroth entry (i.e left-hand dense).
// We try to minimise the number of partially complete entry bundles by writing entries in chunks rather
// than one-by-one.
//
// TODO(#21): Separate sequencing and integration for better performance.
func (a *appender) sequenceBatch(ctx context.Context, entries []*tessera.Entry) error {
	// Return when there is no entry to sequence.
	if len(entries) == 0 {
		return nil
	}

	// Get a Tx for making transaction requests.
	tx, err := a.s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %v", err)
	}
	// Defer a rollback in case anything fails.
	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			klog.Errorf("Failed to rollback in sequenceBatch: %v", err)
		}
	}()

	// Get tree size. Note that "SELECT ... FOR UPDATE" is used for row-level locking.
	row := tx.QueryRowContext(ctx, selectTreeStateByIDForUpdateSQL, treeStateID)
	if err := row.Err(); err != nil {
		return fmt.Errorf("select tree state: %v", err)
	}
	state := treeState{}
	if err := row.Scan(&state.size, &state.root); err != nil {
		return fmt.Errorf("failed to read tree state: %w", err)
	}

	// Integrate the new entries into the entry bundle (TiledLeaves table) and tile (Subtree table).
	if err := a.appendEntries(ctx, tx, state.size, entries); err != nil {
		return fmt.Errorf("failed to integrate: %w", err)
	}

	// Commit the transaction.
	err = tx.Commit()

	select {
	case a.cpUpdated <- struct{}{}:
	default:
	}

	return err
}

// appendEntries incorporates the provided entries into the log starting at fromSeq.
func (a *appender) appendEntries(ctx context.Context, tx *sql.Tx, fromSeq uint64, entries []*tessera.Entry) error {

	sequencedEntries := make([]storage.SequencedEntry, len(entries))
	// Assign provisional sequence numbers to entries.
	// We need to do this here in order to support serialisations which include the log position.
	for i, e := range entries {
		sequencedEntries[i] = storage.SequencedEntry{
			BundleData: e.MarshalBundleData(fromSeq + uint64(i)),
			LeafHash:   e.LeafHash(),
		}
	}

	// Add sequenced entries to entry bundles.
	bundleIndex, entriesInBundle := fromSeq/layout.EntryBundleWidth, fromSeq%layout.EntryBundleWidth
	bundleWriter := &bytes.Buffer{}

	// If the latest bundle is partial, we need to read the data it contains in for our newer, larger, bundle.
	if entriesInBundle > 0 {
		row := tx.QueryRowContext(ctx, selectTiledLeavesSQL, bundleIndex)
		if err := row.Err(); err != nil {
			return fmt.Errorf("query tiled leaves: %v", err)
		}

		var size uint32
		var partialEntryBundle []byte
		if err := row.Scan(&size, &partialEntryBundle); err != nil {
			return fmt.Errorf("scan partial entry bundle: %w", err)
		}
		if size != uint32(entriesInBundle) {
			return fmt.Errorf("expected %d entries in storage but found %d", entriesInBundle, size)
		}

		if _, err := bundleWriter.Write(partialEntryBundle); err != nil {
			return fmt.Errorf("write partial entry bundle: %w", err)
		}
	}

	// Add new entries to the bundle.
	for _, e := range sequencedEntries {
		if _, err := bundleWriter.Write(e.BundleData); err != nil {
			return fmt.Errorf("write bundle data: %w", err)
		}
		entriesInBundle++

		// This bundle is full, so we need to write it out.
		if entriesInBundle == layout.EntryBundleWidth {
			if err := a.s.writeEntryBundle(ctx, tx, bundleIndex, uint32(entriesInBundle), bundleWriter.Bytes()); err != nil {
				return fmt.Errorf("writeEntryBundle: %w", err)
			}

			// Prepare the next entry bundle for any remaining entries in the batch.
			bundleIndex++
			entriesInBundle = 0
			bundleWriter = &bytes.Buffer{}
		}
	}

	// If we have a partial bundle remaining once we've added all the entries from the batch,
	// this needs writing out too.
	if entriesInBundle > 0 {
		if err := a.s.writeEntryBundle(ctx, tx, bundleIndex, uint32(entriesInBundle), bundleWriter.Bytes()); err != nil {
			return fmt.Errorf("writeEntryBundle: %w", err)
		}
	}

	lh := make([][]byte, len(sequencedEntries))
	for i, e := range sequencedEntries {
		lh[i] = e.LeafHash
	}
	newSize, newRoot, err := integrate(ctx, tx, fromSeq, lh, a.s.writeTile)
	if err != nil {
		return fmt.Errorf("integrate: %v", err)
	}

	// Write new tree state.
	if err := a.s.writeTreeState(ctx, tx, newSize, newRoot); err != nil {
		return fmt.Errorf("writeCheckpoint: %w", err)
	}

	klog.Infof("New tree: %d, %x", newSize, newRoot)
	return nil
}

func getTiles(ctx context.Context, tx *sql.Tx, tileIDs []storage.TileID, _ uint64) ([]*api.HashTile, error) {
	hashTiles := make([]*api.HashTile, len(tileIDs))
	if len(tileIDs) == 0 {
		return hashTiles, nil
	}

	// Build the SQL and args to fetch the hash tiles.
	var sql strings.Builder
	args := make([]any, 0, len(tileIDs)*2)
	for i, id := range tileIDs {
		if i != 0 {
			sql.WriteString(" UNION ALL ")
		}
		_, err := sql.WriteString(selectSubtreeByLevelAndIndexSQL)
		if err != nil {
			return nil, err
		}
		args = append(args, id.Level, id.Index)
	}

	rows, err := tx.QueryContext(ctx, sql.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query the hash tiles with SQL (%s): %w", sql.String(), err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			klog.Warningf("Failed to close the rows: %v", err)
		}
	}()

	i := 0
	for rows.Next() {
		var tile []byte
		if err := rows.Scan(&tile); err != nil {
			return nil, fmt.Errorf("scan subtree tile: %w", err)
		}
		t := &api.HashTile{}
		if err := t.UnmarshalText(tile); err != nil {
			return nil, fmt.Errorf("unmarshal tile: %w", err)
		}
		hashTiles[i] = t
		i++
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error while fetching subtrees: %w", err)
	}

	return hashTiles, nil
}

// integrate adds the provided leaf hashes to the merkle tree, starting at the provided location.
func integrate(ctx context.Context, tx *sql.Tx, fromSeq uint64, lh [][]byte, writeTile func(context.Context, *sql.Tx, uint64, uint64, []byte) error) (uint64, []byte, error) {
	getTiles := func(ctx context.Context, tileIDs []storage.TileID, treeSize uint64) ([]*api.HashTile, error) {
		return getTiles(ctx, tx, tileIDs, treeSize)
	}
	newSize, newRoot, tiles, err := storage.Integrate(ctx, getTiles, fromSeq, lh)
	if err != nil {
		return 0, nil, fmt.Errorf("storage.Integrate: %v", err)
	}
	for k, v := range tiles {
		nodes, err := v.MarshalText()
		if err != nil {
			return 0, nil, err
		}

		if err := writeTile(ctx, tx, uint64(k.Level), k.Index, nodes); err != nil {
			return 0, nil, fmt.Errorf("failed to set tile(%v): %w", k, err)
		}
	}

	return newSize, newRoot, nil
}

// MigrationWriter creates a new MySQL storage for the MigrationTarget lifecycle mode.
func (s *Storage) MigrationWriter(ctx context.Context, opts *tessera.MigrationOptions) (migrate.MigrationWriter, tessera.LogReader, error) {
	if err := s.maybeInitTree(ctx); err != nil {
		return nil, nil, fmt.Errorf("maybeInitTree: %v", err)
	}

	return &MigrationStorage{
		s:            s,
		bundleHasher: opts.LeafHasher(),
	}, s, nil
}

// MigrationStorgage implements the tessera.MigrationTarget lifecycle contract.
type MigrationStorage struct {
	s            *Storage
	bundleHasher func([]byte) ([][]byte, error)
}

var _ migrate.MigrationWriter = &MigrationStorage{}

// AwaitIntegration blocks until the local integrated tree has grown to the provided size.
//
// This implements part of the tessera MigrationTarget lifecycle contract.
//
// As well as waiting for the integration to reach the desired size, this method is where
// the integration process itself actually happens.
func (m *MigrationStorage) AwaitIntegration(ctx context.Context, sourceSize uint64) ([]byte, error) {
	// fromSeq keeps track of where we need to integrate from - i.e. the current local size of the integrated tree.
	var fromSeq uint64
	// rows provides a stream of entry bundle rows which will be processed in the loop below.
	var rows *sql.Rows

	// The outer loop "tryAgain", will (re-) setup the streaming read of entry bundles from the DB.
	// The inner loop will go around attempting to process each of these rows in turn. If it encounters
	// a problem it'll break out to the outer loop to sort things out and retry.
tryAgain:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}

		// Release resources if we're going around and resetting the read.
		if rows != nil {
			_ = rows.Close()
		}
		// Figure out where we should be integration from.
		from, err := m.IntegratedSize(ctx)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			klog.Warningf("AwaitIntegration: readTreeState: %v", err)
			continue
		}
		fromSeq = from
		klog.Infof("AwaitIntegration: Integrate from %d (Target %d)", fromSeq, sourceSize)

		// Set up the streaming read of entry bundles from the DB.
		nextBundle := fromSeq / layout.EntryBundleWidth
		rows, err = m.s.db.QueryContext(ctx, streamTiledLeavesSQL, nextBundle)
		if err != nil {
			klog.Warningf("Failed to start streaming entry bundles @%d: %v", nextBundle, err)
			continue
		}

		// This is the inner loop which processes each of the entry bundle rows from the DB read in turn.
		for rows.Next() {
			// Parse the row.
			var idx, size uint64
			var data []byte
			if err := rows.Scan(&idx, &size, &data); err != nil {
				klog.Warningf("AwaitIntegration: Scan: %v", err)
				continue tryAgain
			}
			// Check that we're seeing contiguous bundles, and go around if we've encountered a gap.
			// This isn't necessarily an unrecoverable error, it's probably just that we've either hit the end of all
			// available entry bundles, or whatever process is copying them over hasn't yet written this one.
			// We'll continue looping around in the outer loop (where we back off to avoid hammering the DB) until
			// this entry bundle turns up.
			if want := fromSeq / uint64(layout.EntryBundleWidth); idx != want {
				klog.V(1).Infof("AwaitIntegration: encountered gap, want idx %d (fromSeq %d) but found %d", want, fromSeq, idx)
				continue tryAgain
			}

			// Turn the entry bundle into leaf hashes.
			lh, err := m.bundleHasher(data)
			if err != nil {
				klog.Warningf("AwaitIntegration: bundleHasher: %v", err)
				continue tryAgain
			}

			// Trim the bundle if we've previously integrated some of it (e.g. because it was a [smaller] partial bundle last time
			// we saw it.
			f := fromSeq % layout.EntryBundleWidth
			lh = lh[f:]

			// And finally integrate the bundle into the tree.
			newSize, newRoot, err := m.integrateBatch(ctx, fromSeq, lh)
			if err != nil {
				klog.Warningf("AwaitIntegration: integrateBatch: %v", err)
				continue tryAgain
			}
			fromSeq = newSize

			if newSize == sourceSize {
				klog.Infof("AwaitIntegration: Integrated to %d with root hash %x", newSize, newRoot)
				return newRoot, nil
			}
		}
	}
}

// integrateBatch integrates the provided entries at the specified starting index.
//
// Returns the new size of the local tree and its new root hash.
func (m *MigrationStorage) integrateBatch(ctx context.Context, fromSeq uint64, lh [][]byte) (uint64, []byte, error) {
	tx, err := m.s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		if tx != nil {
			if err := tx.Rollback(); err != nil {
				klog.Warningf("integrateBatch: Rollback: %v", err)
			}
		}
	}()

	newSize, newRoot, err := integrate(ctx, tx, fromSeq, lh, m.s.writeTile)
	if err != nil {
		return 0, nil, fmt.Errorf("integrate: %v", err)
	}
	if err := m.s.writeTreeState(ctx, tx, newSize, newRoot); err != nil {
		return 0, nil, fmt.Errorf("writeTreeState: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, nil, fmt.Errorf("commit: %v", err)
	}
	tx = nil

	return newSize, newRoot, err
}

// SetEntryBundle stores the provided serialised entry bundle at the location implied by the provided
// entry bundle index and partial size.
//
// Implements the tessera MigrationTarget lifecycle contract.
func (m *MigrationStorage) SetEntryBundle(ctx context.Context, index uint64, partial uint8, bundle []byte) error {
	return m.s.writeEntryBundle(ctx, m.s.db, index, uint32(partial), bundle)
}

// IntegratedSize returns the current size of the locally integrated log.
//
// Implements the tessera MigrationTarget lifecycle contract.
func (m *MigrationStorage) IntegratedSize(ctx context.Context) (uint64, error) {
	return m.s.IntegratedSize(ctx)
}
