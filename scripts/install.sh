#!/usr/bin/env bash
set -euo pipefail

cd "$( dirname "${BASH_SOURCE[0]}" )/.."
mkdir -p bin

TARGET_OS=${1:-darwin}
EXTRA_BUILD_OPTIONS=""

if [ "$TARGET_OS" == "darwin" ]
then
  EXTRA_BUILD_OPTIONS="-tags dynamic"
fi

echo "Target OS: $TARGET_OS"
for b in $(ls cmd); do
  for c in $(ls cmd/$b); do
    echo -n "Building $b/$c..."
    GOOS=$TARGET_OS go build $EXTRA_BUILD_OPTIONS -mod=vendor -o "bin/$b/$c" "cmd/$b/$c/main.go"
    echo "done"
  done
done
