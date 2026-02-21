#!/bin/bash
set -e

CONFIGURATION="Debug"
CARGO_FLAGS=""

if [[ "$1" == "--release" ]]; then
    CONFIGURATION="Release"
    CARGO_FLAGS="--release"
fi

echo "=== Building Rust native library ($CONFIGURATION) ==="
cargo build --manifest-path pinvoke/Cargo.toml $CARGO_FLAGS

echo "=== Building C# solution ($CONFIGURATION) ==="
dotnet build --configuration $CONFIGURATION

echo "=== Build complete ==="
