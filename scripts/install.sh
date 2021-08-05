#!/usr/bin/env bash
set -exuo pipefail

cd "$( dirname "${BASH_SOURCE[0]}" )/.."
mkdir -p bin

TARGET_OS=${1:-darwin}

echo "Target OS: $TARGET_OS"
for b in $(ls cmd); do
  echo -n "Building $b..."
  GOOS=$TARGET_OS go build -mod=vendor -o bin/$b cmd/$b/main.go
  echo "done"
done
