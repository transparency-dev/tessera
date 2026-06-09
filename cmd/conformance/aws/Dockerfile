FROM golang:1.25.11-alpine3.23@sha256:c05ba4b73604069d376c4f41346b05374335b5ca0c46fb6dfede5a59f5196931 AS builder

ARG GOFLAGS="-trimpath -buildvcs=false -buildmode=exe"
ENV GOFLAGS=$GOFLAGS

# Move to working directory /build
WORKDIR /build

# Copy and download dependencies using go mod
COPY go.mod .
COPY go.sum .
RUN go mod download

# Copy the code into the container
COPY . .

# Build the application
RUN go build -o bin/conformance-aws ./cmd/conformance/aws

# Build release image
FROM alpine:3.20.2@sha256:0a4eaa0eecf5f8c050e5bba433f58c052be7587ee8af3e8b3910ef9ab5fbe9f5

COPY --from=builder /build/bin/conformance-aws /bin/conformance-aws
ENTRYPOINT ["/bin/conformance-aws"]
