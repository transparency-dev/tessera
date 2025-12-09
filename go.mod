module github.com/transparency-dev/tessera

go 1.24.0

require (
	cloud.google.com/go/spanner v1.86.1
	cloud.google.com/go/storage v1.58.0
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.54.0
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace v1.30.0
	github.com/RobinUS2/golang-moving-average v1.0.0
	github.com/avast/retry-go/v4 v4.7.0
	github.com/aws/aws-sdk-go-v2 v1.41.0
	github.com/aws/aws-sdk-go-v2/config v1.32.4
	github.com/aws/aws-sdk-go-v2/credentials v1.19.4
	github.com/aws/aws-sdk-go-v2/service/s3 v1.93.1
	github.com/aws/smithy-go v1.24.0
	github.com/bitfield/script v0.24.1
	github.com/cenkalti/backoff/v5 v5.0.3
	github.com/charmbracelet/bubbletea v1.3.10
	github.com/charmbracelet/lipgloss v1.1.0
	github.com/dgraph-io/badger/v4 v4.8.0
	github.com/dustin/go-humanize v1.0.1
	github.com/gdamore/tcell/v2 v2.13.2
	github.com/google/go-cmp v0.7.0
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/muesli/termenv v0.16.0
	github.com/rivo/tview v0.42.0
	github.com/transparency-dev/formats v0.0.0-20251017110053-404c0d5b696c
	github.com/transparency-dev/merkle v0.0.2
	go.opentelemetry.io/contrib/detectors/aws/ec2/v2 v2.0.0
	go.opentelemetry.io/contrib/detectors/aws/ecs v1.38.0
	go.opentelemetry.io/contrib/detectors/gcp v1.38.0
	go.opentelemetry.io/contrib/propagators/aws v1.38.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.39.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.39.0
	go.opentelemetry.io/otel/sdk v1.39.0
	go.opentelemetry.io/otel/sdk/metric v1.39.0
	golang.org/x/exp v0.0.0-20240325151524-a685a6edb6d8
	golang.org/x/mod v0.31.0
	google.golang.org/api v0.257.0
	google.golang.org/grpc v1.77.0
	k8s.io/klog/v2 v2.130.1
)

require (
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/monitoring v1.24.2 // indirect
	cloud.google.com/go/trace v1.11.6 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.30.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.54.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.4 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.4 // indirect
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/brunoscheufler/aws-ecs-metadata-go v0.0.0-20221221133751-67e37ae746cd // indirect
	github.com/charmbracelet/colorprofile v0.2.3-0.20250311203215-f60798e515dc // indirect
	github.com/charmbracelet/x/ansi v0.10.2 // indirect
	github.com/charmbracelet/x/cellbuf v0.0.13-0.20250311204145-2c3ea96c31dd // indirect
	github.com/charmbracelet/x/term v0.2.1 // indirect
	github.com/dgraph-io/ristretto/v2 v2.2.0 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.35.0 // indirect
	github.com/erikgeiser/coninput v0.0.0-20211004153227-1c3628e74d0f // indirect
	github.com/go-jose/go-jose/v4 v4.1.3 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.3 // indirect
	github.com/itchyny/gojq v0.12.13 // indirect
	github.com/itchyny/timefmt-go v0.1.5 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-localereader v0.0.1 // indirect
	github.com/muesli/ansi v0.0.0-20230316100256-276c6243b2f6 // indirect
	github.com/muesli/cancelreader v0.2.2 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.63.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.39.0 // indirect
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	mvdan.cc/sh/v3 v3.7.0 // indirect
)

require (
	cel.dev/expr v0.24.0 // indirect
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth v0.17.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/iam v1.5.3 // indirect
	cloud.google.com/go/longrunning v0.7.0 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/GoogleCloudPlatform/grpc-gcp-go/grpcgcp v1.5.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cncf/xds/go v0.0.0-20251022180443-0feb69152e9f // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/gdamore/encoding v1.0.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-sql-driver/mysql v1.9.3
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.7 // indirect
	github.com/googleapis/gax-go/v2 v2.15.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.3.0 // indirect
	github.com/mattn/go-runewidth v0.0.17 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.63.0 // indirect
	go.opentelemetry.io/otel v1.39.0
	go.opentelemetry.io/otel/metric v1.39.0
	go.opentelemetry.io/otel/trace v1.39.0 // indirect
	golang.org/x/crypto v0.46.0
	golang.org/x/net v0.47.0
	golang.org/x/oauth2 v0.33.0 // indirect
	golang.org/x/sync v0.19.0
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/term v0.38.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	golang.org/x/time v0.14.0
	google.golang.org/genproto v0.0.0-20250922171735-9219d122eba9 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)
