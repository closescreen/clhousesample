#!/usr/bin/env bash
#> возвращает 1 если период полон (не нужно добавлять) и 0 если нужно заливать
set -u
set +x
set -o pipefail
cd `dirname $0`

export PERL5LIB=${PERL5LIB:-""}:/usr/local/rle/var/share3/TIKETS/bike

serv=`hostname`

max_days=21 # проверять что хранится не более этого 
#> NOTE: Удаление старых дней может задерживаться из-за больших кусков, 
#> поэтому устанавливать <max_days> нужно больше, чем реально нужно (дней на 7).

db=${1:?DB!} #"rnd600"
table=${2:?TABLE!} #"history_ref"


has_days=`khpost -q"SELECT DISTINCT d from $db.$table" | words`
has_count_days=`echo "$has_days" | words -count`
[[ -z "$has_count_days" ]] && has_count_days=0

# если имеется столько дней, больше не добавлять:
if [[ $has_count_days -ge $max_days ]]; then
    echo "have count days:$has_count_days > need:$max_days (have days: $has_days)">&2
    exit 1
else
    true
    exit 0
    #echo "OK: have count days: $has_count_days less then need: $max_days">&2
fi




