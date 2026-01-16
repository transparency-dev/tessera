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

// aws is a simple personality allowing to run conformance/compliance/performance tests and showing how to use the Tessera AWS storage implementation.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"

	aaws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-sql-driver/mysql"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/storage/aws"
	aws_as "github.com/transparency-dev/tessera/storage/aws/antispam"
	"golang.org/x/mod/sumdb/note"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"k8s.io/klog/v2"
)

var (
	bucket            = flag.String("bucket", "", "Bucket to use for storing log")
	dbName            = flag.String("db_name", "", "AuroraDB name for the log DB")
	dbHost            = flag.String("db_host", "", "AuroraDB host")
	dbPort            = flag.Int("db_port", 3306, "AuroraDB port")
	dbUser            = flag.String("db_user", "", "AuroraDB user")
	dbPassword        = flag.String("db_password", "", "AuroraDB user")
	dbMaxConns        = flag.Int("db_max_conns", 0, "Maximum connections to the database, defaults to 0, i.e unlimited")
	dbMaxIdle         = flag.Int("db_max_idle_conns", 2, "Maximum idle database connections in the connection pool, defaults to 2")
	s3Endpoint        = flag.String("s3_endpoint", "", "Endpoint for custom non-AWS S3 service")
	s3AccessKeyID     = flag.String("s3_access_key", "", "Access key ID for custom non-AWS S3 service")
	s3SecretAccessKey = flag.String("s3_secret", "", "Secret access key for custom non-AWS S3 service")

	listen            = flag.String("listen", ":2024", "Address:port to listen on")
	signer            = flag.String("signer", "", "Note signer to use to sign checkpoints")
	publishInterval   = flag.Duration("publish_interval", 3*time.Second, "How frequently to publish updated checkpoints")
	traceFraction     = flag.Float64("trace_fraction", 0, "Fraction of open-telemetry span traces to sample")
	additionalSigners = []string{}

	antispamEnable = flag.Bool("antispam", false, "EXPERIMENTAL: Set to true to enable persistent antispam storage")
	antispamDb     = flag.String("antispam_db_name", "", "AuroraDB name for the antispam DB")
)

func init() {
	flag.Func("additional_signer", "Additional note signer for checkpoints, may be specified multiple times", func(s string) error {
		additionalSigners = append(additionalSigners, s)
		return nil
	})
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	ctx := context.Background()

	shutdownOTel := initOTel(ctx, *traceFraction)
	defer shutdownOTel(ctx)
	s, a := signerFromFlags()

	// Create our Tessera storage backend:
	awsCfg := storageConfigFromFlags()
	driver, err := aws.New(ctx, awsCfg)
	if err != nil {
		klog.Exitf("Failed to create new AWS storage: %v", err)
	}
	var antispam tessera.Antispam
	// Persistent antispam is currently experimental, so there's no documentation yet!
	if *antispamEnable {
		asOpts := aws_as.AntispamOpts{} // Use defaults
		antispam, err = aws_as.NewAntispam(ctx, antispamMysqlConfig().FormatDSN(), asOpts)
		if err != nil {
			klog.Exitf("Failed to create new AWS antispam storage: %v", err)
		}
	}
	appender, shutdown, _, err := tessera.NewAppender(ctx, driver, tessera.NewAppendOptions().
		WithCheckpointSigner(s, a...).
		WithCheckpointInterval(*publishInterval).
		WithBatching(512, 300*time.Millisecond).
		WithPushback(10*4096).
		WithAntispam(tessera.DefaultAntispamInMemorySize, antispam))
	if err != nil {
		klog.Exit(err)
	}

	// Expose a HTTP handler for the conformance test writes.
	// This should accept arbitrary bytes POSTed to /add, and return an ascii
	// decimal representation of the index assigned to the entry.
	http.HandleFunc("POST /add", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		idx, err := appender.Add(r.Context(), tessera.NewEntry(b))()
		if err != nil {
			if errors.Is(err, tessera.ErrPushback) {
				w.Header().Add("Retry-After", "1")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		// Write out the assigned index
		_, _ = fmt.Fprintf(w, "%d", idx.Index)
	})

	h2s := &http2.Server{}
	h1s := &http.Server{
		Addr:              *listen,
		Handler:           h2c.NewHandler(http.DefaultServeMux, h2s),
		ReadHeaderTimeout: 5 * time.Second,
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

// storageConfigFromFlags returns an aws.Config struct populated with values
// provided via flags.
func storageConfigFromFlags() aws.Config {
	if *bucket == "" {
		klog.Exit("--bucket must be set")
	}
	if *dbName == "" {
		klog.Exit("--db_name must be set")
	}
	if *dbHost == "" {
		klog.Exit("--db_host must be set")
	}
	if *dbPort == 0 {
		klog.Exit("--db_port must be set")
	}
	if *dbUser == "" {
		klog.Exit("--db_user must be set")
	}
	// Empty password isn't an option with AuroraDB MySQL.
	if *dbPassword == "" {
		klog.Exit("--db_password must be set")
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

func antispamMysqlConfig() *mysql.Config {
	if *antispamDb == "" {
		klog.Exit("--antispam_db_name must be set")
	}
	if *dbHost == "" {
		klog.Exit("--db_host must be set")
	}
	if *dbPort == 0 {
		klog.Exit("--db_port must be set")
	}
	if *dbUser == "" {
		klog.Exit("--db_user must be set")
	}
	// Empty password isn't an option with AuroraDB MySQL.
	if *dbPassword == "" {
		klog.Exit("--db_password must be set")
	}

	return &mysql.Config{
		User:                    *dbUser,
		Passwd:                  *dbPassword,
		Net:                     "tcp",
		Addr:                    fmt.Sprintf("%s:%d", *dbHost, *dbPort),
		DBName:                  *antispamDb,
		AllowCleartextPasswords: true,
		AllowNativePasswords:    true,
	}
}

func signerFromFlags() (note.Signer, []note.Signer) {
	s, err := note.NewSigner(*signer)
	if err != nil {
		klog.Exitf("Failed to create new signer: %v", err)
	}

	var a []note.Signer
	for _, as := range additionalSigners {
		s, err := note.NewSigner(as)
		if err != nil {
			klog.Exitf("Failed to create additional signer: %v", err)
		}
		a = append(a, s)
	}

	return s, a
}
