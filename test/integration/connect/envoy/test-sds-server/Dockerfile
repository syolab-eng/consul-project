# Copyright IBM Corp. 2024, 2026
# SPDX-License-Identifier: BUSL-1.1

# CI Dockerfile: build from source using the Go toolchain image.
# Pass GOLANG_VERSION from the first line of the repo .go-version file so
# this stays on the official golang (Debian) image for that toolchain
# instead of floating on golang:latest.
# For local development where Docker Hub pulls are blocked by credential
# helpers, use Dockerfile.local (selected automatically by run-tests.sh
# when LOCAL_BUILD=1).
ARG GOLANG_VERSION
FROM golang:${GOLANG_VERSION}

WORKDIR /go/src
COPY ./ .

RUN go build -v -o test-sds-server sds.go

CMD ["/go/src/test-sds-server"]
