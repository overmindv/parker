#!/usr/bin/env bash
set -euo pipefail
version=${1:?usage: verify-release.sh vMAJOR.MINOR.PATCH}
[[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo 'invalid release version' >&2; exit 1; }
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export GOWORK=off
cd "$root"
go build -o "$work/parker" ./cmd/parker
test "$("$work/parker" version)" = "parker ${version#v}"
mkdir "$work/consumer"
cd "$work/consumer"
"$work/parker" init release-consumer --pg >/dev/null
go get "github.com/overmindv/parker@$version"
go mod tidy
test "$(go list -m -f '{{.Version}}' github.com/overmindv/parker)" = "$version"
test "$(go list -m -f '{{if .Replace}}replace{{end}}' github.com/overmindv/parker)" = ''
go build ./...
go test -race ./...
test "$(go run ./cmd/release-consumer version)" = "parker ${version#v}"
printf 'Verified published Parker %s without replace\n' "$version"
