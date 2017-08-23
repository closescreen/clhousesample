#!/usr/bin/env bash

set -u
set -x
set -o pipefail

f=${1:?FILE!}
ug_from=${2:? ug_from!}
ug_to=${3:? ug_to!}
deb=${4:-""}
[[ -n "$deb" ]] && set -x

./to_history_ref_drop_create_hour.sh "$f"
./to_history_ref_insert.sh "$f" "$ug_from" "$ug_to"
./to_history_ref_attach_from_hour.sh "$f" "$deb"
ok=$?

./to_history_ref_drop_hour.sh "$f" 

exit $ok