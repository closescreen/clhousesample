#!/usr/bin/env bash
#> Сохраняет из history_ref_from_history_v04.py в clickhouse history_ref_DAY
#> Один процесс на день. День либо залился, либо нет. 
#> Если нет или частично - таблица удаляется и наливается заново.
#(
set -u
set +x
set -o pipefail
cd `dirname $0`

export PERL5LIB=${PERL5LIB:-""}:/usr/local/rle/var/share3/TIKETS/bike

# single process! --wait must be enabled for return status after washing
serv=`hostname`
if [[ ${1:-""} == "start" ]];then shift && nice fork -pf="$0.$serv.pids" --single "$0 $@"  --wait --status # enable -wait 
elif [[ ${1:-""}  == "stop" ]];then shift && fork -pf="$0.$serv.pids" -kila
else 
# --------- begin of script body ----

# Параметры:
day=${1?DAY!}
deb=${2:-""} # можно указать "0" (нет debug) / "1" (info) / "2" или "deb" (максимум отладки)
[[ -z "$deb" ]] && deb=0; # - debug  off
[[ "$deb" != "0" ]] && [[ "$deb" != "1" ]] && deb=2 # full debug level
washdeb="" # указание debug для washing
[[ "$deb" == "2" ]] && washdeb="-d" && set -x

my_server=`hostname` # dm22
db="rnd600"
main_table="history_ref"

# каждый сервер хранит свои usergroups
ug_from="" && ug_to=""
[[ "$my_server" == "dm22" ]] && ug_from=1 && ug_to=128
[[ "$my_server" == "dm23" ]] && ug_from=129 && ug_to=256
[[ -z "$ug_from" ]] && echo "ug_from!">&2 && exit 2


d="$day"
#day_table="${main_table}_${d//-/_}" # like: history_ref_2015_05_16


hours -t=$day -n=24 | files "../../reg_history_ref/%F/%H_${my_server}.txt" |
    washing $washdeb -r='[[ -s %f ]]' -comm="результ. файлы проверяются на непусто" \
	-cmd=" ./to_history_ref_hour.sh \"%f\" \"$ug_from\" \"$ug_to\" " || exit 1  # ВЫХОД при ошибке


# --------- end of script bidy ------ 
fi

#)>>"$0.log" 2>&1
