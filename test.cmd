@echo off
setlocal

set CONFIGURATION=Debug

if "%1"=="--release" (
    set CONFIGURATION=Release
)

echo === Running Rust tests ===
cd pinvoke
cargo test
if errorlevel 1 exit /b 1
cd ..

echo === Running C# tests (%CONFIGURATION%) ===
dotnet test --no-build --configuration %CONFIGURATION% --verbosity normal --logger "console;verbosity=detailed"
if errorlevel 1 exit /b 1

echo === All tests passed ===
