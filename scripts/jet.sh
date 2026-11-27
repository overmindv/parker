#!/bin/sh
# Jet очищает выходной каталог, поэтому всегда запускаем его в отдельной директории.
set -eu
action=${1:?expected generate or check}
target=${2:-internal/adapter/postgres/generated}
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
go run ./internal/cmd/jetgen "$stage"

# Пустая или неполная БД не должна затереть существующие модели.
for package in model table; do
    for name in outbox outbox_replay_audit; do
        test -s "$stage/$package/$name.go" || {
            echo "Missing generated $package/$name.go; check the outbox migrations." >&2
            exit 1
        }
    done
done

case "$action" in
    check) diff -ru "$target" "$stage" ;;
    generate)
        for package in model table; do
            mkdir -p "$target/$package"
            cp "$stage/$package/"*.go "$target/$package/"
        done
        ;;
    *) echo 'expected generate or check' >&2; exit 2 ;;
esac
