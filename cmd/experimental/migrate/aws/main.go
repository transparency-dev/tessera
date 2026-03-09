// Copyright 2025 The Tessera authors. All Rights Reserved.
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

// aws-migrate is a command-line tool for migrating data from a tlog-tiles
// compliant log, into a Tessera log instance hosted on AWS.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"

	"log/slog"

	aaws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-sql-driver/mysql"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/internal/parse"
	"github.com/transparency-dev/tessera/storage/aws"
)

var (
	bucket            = flag.String("bucket", "", "Bucket to use for storing log")
	dbName            = flag.String("db_name", "", "AuroraDB name")
	dbHost            = flag.String("db_host", "", "AuroraDB host")
	dbPort            = flag.Int("db_port", 3306, "AuroraDB port")
	dbUser            = flag.String("db_user", "", "AuroraDB user")
	dbPassword        = flag.String("db_password", "", "AuroraDB user")
	dbMaxConns        = flag.Int("db_max_conns", 0, "Maximum connections to the database, defaults to 0, i.e unlimited")
	dbMaxIdle         = flag.Int("db_max_idle_conns", 2, "Maximum idle database connections in the connection pool, defaults to 2")
	s3Endpoint        = flag.String("s3_endpoint", "", "Endpoint for custom non-AWS S3 service")
	s3AccessKeyID     = flag.String("s3_access_key", "", "Access key ID for custom non-AWS S3 service")
	s3SecretAccessKey = flag.String("s3_secret", "", "Secret access key for custom non-AWS S3 service")

	sourceURL  = flag.String("source_url", "", "Base URL for the source log.")
	numWorkers = flag.Uint("num_workers", 30, "Number of migration worker goroutines.")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	if *sourceURL == "" {
		slog.Error("Missing parameter: --source_url")
		os.Exit(255)
	}
	srcURL, err := url.Parse(*sourceURL)
	if err != nil {
		slog.Error("Invalid --source_url", slog.String("param", *sourceURL), slog.Any("error", err))
		os.Exit(255)
	}
	src, err := client.NewHTTPFetcher(srcURL, nil)
	if err != nil {
		slog.Error("Failed to create HTTP fetcher", slog.Any("error", err))
		os.Exit(255)
	}
	sourceCP, err := src.ReadCheckpoint(ctx)
	if err != nil {
		slog.Error("fetch initial source checkpoint", slog.Any("error", err))
		os.Exit(255)
	}
	// TODO(mhutchinson): parse this safely.
	_, sourceSize, sourceRoot, err := parse.CheckpointUnsafe(sourceCP)
	if err != nil {
		slog.Error("Failed to parse checkpoint", slog.Any("error", err))
		os.Exit(255)
	}

	// Create our Tessera storage backend:
	awsCfg := storageConfigFromFlags()
	driver, err := aws.New(ctx, awsCfg)
	if err != nil {
		slog.Error("Failed to create new AWS storage", slog.Any("error", err))
		os.Exit(255)
	}
	opts := tessera.NewMigrationOptions()

	m, err := tessera.NewMigrationTarget(ctx, driver, opts)
	if err != nil {
		slog.Error("Failed to create MigrationTarget", slog.Any("error", err))
		os.Exit(255)
	}

	slog.Info("Starting Migrate() with workers=, sourceSize=, migrating from", slog.Any("numworkers", *numWorkers), slog.Uint64("sourcesize", sourceSize), slog.String("sourceurl", *sourceURL))
	if err := m.Migrate(context.Background(), *numWorkers, sourceSize, sourceRoot, src.ReadEntryBundle); err != nil {
		slog.Error("Migrate failed", slog.Any("error", err))
		os.Exit(255)
	}
}

// storageConfigFromFlags returns an aws.Config struct populated with values
// provided via flags.
func storageConfigFromFlags() aws.Config {
	if *bucket == "" {
		slog.Error("--bucket must be set")
		os.Exit(255)
	}
	if *dbName == "" {
		slog.Error("--db_name must be set")
		os.Exit(255)
	}
	if *dbHost == "" {
		slog.Error("--db_host must be set")
		os.Exit(255)
	}
	if *dbPort == 0 {
		slog.Error("--db_port must be set")
		os.Exit(255)
	}
	if *dbUser == "" {
		slog.Error("--db_user must be set")
		os.Exit(255)
	}
	// Empty passord isn't an option with AuroraDB MySQL.
	if *dbPassword == "" {
		slog.Error("--db_password must be set")
		os.Exit(255)
	}

	c := mysql.Config{
		User:                    *dbUser,
		Passwd:                  *dbPassword,
		Net:                     "tcp",
		Addr:                    fmt.Sprintf("%s:%d", *dbHost, *dbPort),
		DBName:                  *dbName,
		AllowCleartextPasswords: true,
		AllowNativePasswords:    true,
	}

	// Configure to use MinIO Server
	var awsConfig *aaws.Config
	var s3Opts func(o *s3.Options)
	if *s3Endpoint != "" {
		const defaultRegion = "us-east-1"
		s3Opts = func(o *s3.Options) {
			o.BaseEndpoint = aaws.String(*s3Endpoint)
			o.Credentials = credentials.NewStaticCredentialsProvider(*s3AccessKeyID, *s3SecretAccessKey, "")
			o.Region = defaultRegion
			o.UsePathStyle = true
		}

		awsConfig = &aaws.Config{
			Region: defaultRegion,
		}
	}

	return aws.Config{
		Bucket:       *bucket,
		SDKConfig:    awsConfig,
		S3Options:    s3Opts,
		DSN:          c.FormatDSN(),
		MaxOpenConns: *dbMaxConns,
		MaxIdleConns: *dbMaxIdle,
	}
}
