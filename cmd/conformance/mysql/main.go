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

// mysql is a simple personality allowing to run conformance/compliance/performance tests and showing how to use the Tessera MySQL storage implmentation.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"time"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/storage/mysql"
	"golang.org/x/mod/sumdb/note"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"k8s.io/klog/v2"
)

var (
	mysqlURI                  = flag.String("mysql_uri", "user:password@tcp(db:3306)/tessera", "Connection string for a MySQL database")
	dbConnMaxLifetime         = flag.Duration("db_conn_max_lifetime", 3*time.Minute, "")
	dbMaxOpenConns            = flag.Int("db_max_open_conns", 64, "")
	dbMaxIdleConns            = flag.Int("db_max_idle_conns", 64, "")
	initSchemaPath            = flag.String("init_schema_path", "", "Location of the schema file if database initialization is needed")
	listen                    = flag.String("listen", ":2024", "Address:port to listen on")
	privateKeyPath            = flag.String("private_key_path", "", "Location of private key file")
	publishInterval           = flag.Duration("publish_interval", 3*time.Second, "How frequently to publish updated checkpoints")
	additionalPrivateKeyPaths = []string{}
)

func init() {
	flag.Func("additional_private_key_path", "Location of additional private key file, may be specified multiple times", func(s string) error {
		additionalPrivateKeyPaths = append(additionalPrivateKeyPaths, s)
		return nil
	})
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	ctx := context.Background()

	db := createDatabaseOrDie(ctx)
	noteSigner, additionalSigners := createSignersOrDie()

	// Initialise the Tessera MySQL storage
	driver, err := mysql.New(ctx, db)
	if err != nil {
		klog.Exitf("Failed to create new MySQL storage: %v", err)
	}

	appender, shutdown, reader, err := tessera.NewAppender(ctx, driver, tessera.NewAppendOptions().
		WithCheckpointSigner(noteSigner, additionalSigners...).
		WithCheckpointInterval(*publishInterval).
		WithAntispam(256, nil))
	if err != nil {
		klog.Exit(err)
	}
	// Set up the handlers for the tlog-tiles GET methods, and a custom handler for HTTP POSTs to /add
	configureTilesReadAPI(http.DefaultServeMux, reader)
	http.HandleFunc("POST /add", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		idx, err := appender.Add(r.Context(), tessera.NewEntry(b))()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		if _, err := fmt.Fprintf(w, "%d", idx.Index); err != nil {
			klog.Errorf("/add: %v", err)
			return
		}
	})

	// TODO(mhutchinson): Change the listen flag to just a port, or fix up this address formatting
	klog.Infof("Environment variables useful for accessing this log:\n"+
		"export WRITE_URL=http://localhost%s/ \n"+
		"export READ_URL=http://localhost%s/ \n", *listen, *listen)
	// Run the HTTP server with the single handler and block until this is terminated
	h2s := &http2.Server{}
	h1s := &http.Server{
		Addr:    *listen,
		Handler: h2c.NewHandler(http.DefaultServeMux, h2s),
	}
	if err := http2.ConfigureServer(h1s, h2s); err != nil {
		klog.Exitf("http2.ConfigureServer: %v", err)
	}

	if err := h1s.ListenAndServe(); err != nil {
		if err := shutdown(ctx); err != nil {
			klog.Exit(err)
		}
		klog.Exitf("ListenAndServe: %v", err)
	}
}

func createDatabaseOrDie(ctx context.Context) *sql.DB {
	db, err := sql.Open("mysql", *mysqlURI)
	if err != nil {
		klog.Exitf("Failed to connect to DB: %v", err)
	}
	db.SetConnMaxLifetime(*dbConnMaxLifetime)
	db.SetMaxOpenConns(*dbMaxOpenConns)
	db.SetMaxIdleConns(*dbMaxIdleConns)

	initDatabaseSchema(ctx)
	return db
}

func createSignersOrDie() (note.Signer, []note.Signer) {
	s := createSignerOrDie(*privateKeyPath)
	a := []note.Signer{}
	for _, p := range additionalPrivateKeyPaths {
		a = append(a, createSignerOrDie(p))
	}
	return s, a
}

func createSignerOrDie(s string) note.Signer {
	rawPrivateKey, err := os.ReadFile(s)
	if err != nil {
		klog.Exitf("Failed to read private key file %q: %v", s, err)
	}
	noteSigner, err := note.NewSigner(string(rawPrivateKey))
	if err != nil {
		klog.Exitf("Failed to create new signer: %v", err)
	}
	return noteSigner
}

// configureTilesReadAPI adds the API methods from https://c2sp.org/tlog-tiles to the mux,
// routing the requests to the mysql storage.
// This method could be moved into the storage API as it's likely this will be
// the same for any implementation of a personality based on MySQL.
func configureTilesReadAPI(mux *http.ServeMux, reader tessera.LogReader) {
	mux.HandleFunc("GET /checkpoint", func(w http.ResponseWriter, r *http.Request) {
		checkpoint, err := reader.ReadCheckpoint(r.Context())
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			klog.Errorf("/checkpoint: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Don't cache checkpoints as the endpoint refreshes regularly.
		// A personality that wanted to _could_ set a small cache time here which was no higher
		// than the checkpoint publish interval.
		w.Header().Set("Cache-Control", "no-cache")
		if _, err := w.Write(checkpoint); err != nil {
			klog.Errorf("/checkpoint: %v", err)
			return
		}
	})

	mux.HandleFunc("GET /tile/{level}/{index...}", func(w http.ResponseWriter, r *http.Request) {
		level, index, p, err := layout.ParseTileLevelIndexPartial(r.PathValue("level"), r.PathValue("index"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			if _, werr := fmt.Fprintf(w, "Malformed URL: %s", err.Error()); werr != nil {
				klog.Errorf("/tile/{level}/{index...}: %v", werr)
			}
			return
		}
		tile, err := reader.ReadTile(r.Context(), level, index, p)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			klog.Errorf("/tile/{level}/{index...}: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Cache-Control", "max-age=31536000, immutable")

		if _, err := w.Write(tile); err != nil {
			klog.Errorf("/tile/{level}/{index...}: %v", err)
			return
		}
	})

	mux.HandleFunc("GET /tile/entries/{index...}", func(w http.ResponseWriter, r *http.Request) {
		index, p, err := layout.ParseTileIndexPartial(r.PathValue("index"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			if _, werr := fmt.Fprintf(w, "Malformed URL: %s", err.Error()); werr != nil {
				klog.Errorf("/tile/entries/{index...}: %v", werr)
			}
			return
		}

		entryBundle, err := reader.ReadEntryBundle(r.Context(), index, p)
		if err != nil {
			klog.Errorf("/tile/entries/{index...}: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if entryBundle == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")

		if _, err := w.Write(entryBundle); err != nil {
			klog.Errorf("/tile/entries/{index...}: %v", err)
			return
		}
	})
}

func initDatabaseSchema(ctx context.Context) {
	if *initSchemaPath != "" {
		klog.Infof("Initializing database schema")

		db, err := sql.Open("mysql", *mysqlURI+"?multiStatements=true")
		if err != nil {
			klog.Exitf("Failed to connect to DB: %v", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				klog.Warningf("Failed to close db: %v", err)
			}
		}()

		rawSchema, err := os.ReadFile(*initSchemaPath)
		if err != nil {
			klog.Exitf("Failed to read init schema file %q: %v", *initSchemaPath, err)
		}
		if _, err := db.ExecContext(ctx, string(rawSchema)); err != nil {
			klog.Exitf("Failed to execute init database schema: %v", err)
		}

		klog.Infof("Database schema initialized")
	}
}
