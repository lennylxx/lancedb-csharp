#!/bin/bash
set -e

./build.sh

echo "=== Running tests ==="
dotnet test --no-build --verbosity normal

echo "=== All tests passed ==="
