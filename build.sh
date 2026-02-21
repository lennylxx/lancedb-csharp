#!/bin/bash
set -e

echo "=== Building C# solution (includes Rust native library) ==="
dotnet build

echo "=== Build complete ==="
