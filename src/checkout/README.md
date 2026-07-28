# Checkout Service

This service provides checkout services for the application.

`operation_id` makes checkout retries identify the same order. Payment and
shipping still lack durable saga state and compensation, so a process failure
between side effects can repeat or leave a partially completed order.

## Local Build

To build the service binary, run:

```sh
go build -o /go/bin/checkout/
```

## Docker Build

From the root directory, run:

```sh
docker compose build checkout
```

## Regenerate protos

To build the protos, run from the root directory:

```sh
make docker-generate-protobuf
```

## Generate feature flag types

To regenerate the typed feature flag accessors from `flags.json`, run from the
service directory:

```sh
go generate ./...
```

This uses the [OpenFeature CLI](https://github.com/open-feature/cli) to
produce `flags/flags_gen.go`.

## Bump dependencies

To bump all dependencies run:

```sh
go get -u -t ./...
go mod tidy
```
