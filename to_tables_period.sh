#!/usr/bin/env bash
#>
#(
set -u
set -x
set -o pipefail
cd `dirname $0`

export PERL5LIB=${PERL5LIB:-""}:/usr/local/rle/var/share3/TIKETS/bike

serv=`hostname`
if [[ ${1:-""} == "start" ]];then shift && nice fork -pf="$0.$serv.pids" --single "$0 $*"  --wait --status # enable -wait or redirect out to log
elif [[ ${1:-""}  == "stop" ]];then shift && fork -pf="$0.$serv.pids" -kila
else 
# --------- begin of script body ----

fromday="today"
load_days=13 # сколько дней в прошлое от дня <fromday> (не включительно) начать проверять/загружать сейчас в кликхаус.
debuglevel=1
me=`basename $0`
[[ $debuglevel -eq 2 ]] && set -x

# дни не распараллеливаем, заполняем по одному
for day in `hours -t=${fromday}T00 -n=-${load_days}days -days`; do #  нужные нам дни
    [[ $debuglevel -ge 1 ]] && echo "$me: $day (rnd600 history_ref)">&2
    ./to_table_is_need_fill_period.sh rnd600 history_ref && 
	./to_history_ref_hours_wash.sh "$day" "$debuglevel"
    
    [[ $debuglevel -ge 1 ]] && echo "$me: $day (rnd600 history_uid)">&2
    ./to_table_is_need_fill_period.sh rnd600 history_uid && 
	./history_uid_SQL-generate.sh "$day" | ./history_uid_SQL-run-2.sh "$day" && nice ./history_uid_SQL-run-3.sh "$day"

done 

# --------- end of script bidy ------ 
fi


#)>>"$0.log" 2>&1
