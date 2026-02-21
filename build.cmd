@echo off
setlocal

set CONFIGURATION=Debug
set CARGO_FLAGS=

if "%1"=="--release" (
    set CONFIGURATION=Release
    set CARGO_FLAGS=--release
)

echo === Building Rust native library (%CONFIGURATION%) ===
cargo build --manifest-path pinvoke\Cargo.toml %CARGO_FLAGS%
if errorlevel 1 exit /b 1

echo === Building C# solution (%CONFIGURATION%) ===
dotnet build --configuration %CONFIGURATION%
if errorlevel 1 exit /b 1

echo === Build complete ===
