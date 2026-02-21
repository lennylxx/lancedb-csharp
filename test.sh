#!/bin/bash
set -e

./build.sh

echo "=== Running Rust tests ==="
cd pinvoke && cargo test
cd ..

echo "=== Running C# tests ==="
dotnet test --no-build --verbosity normal --logger "console;verbosity=detailed"

echo "=== All tests passed ==="
