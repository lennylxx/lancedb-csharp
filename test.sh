#!/bin/bash
set -e

CONFIGURATION="Debug"

if [[ "$1" == "--release" ]]; then
    CONFIGURATION="Release"
fi

echo "=== Running Rust tests ==="
cd pinvoke && cargo test
cd ..

echo "=== Running C# tests ($CONFIGURATION) ==="
dotnet test --no-build --configuration $CONFIGURATION --verbosity normal --logger "console;verbosity=detailed"

echo "=== All tests passed ==="
