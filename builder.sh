#!/usr/bin/env bash
set -e
cargo build

rm ./infra ./bunshin ./tester || true

cp -v ./target/debug/infra .
cp -v ./target/debug/bunshin .
cp -v ./target/debug/tester .
