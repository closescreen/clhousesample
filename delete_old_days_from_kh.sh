#!/usr/bin/env bash
#> Удаляет старые дни из таблицы <table> если количество дней в таблице больше <max_days>
#(
set -u
set -x
set -o pipefail
cd `dirname $0`

export PERL5LIB=${PERL5LIB:-""}:/usr/local/rle/var/share3/TIKETS/bike
f=`which fork`
[[ -z "$f" ]] && export PATH=$PATH:/usr/local/rle/var/share3/TIKETS/bike



# один процесс
serv=`hostname`
if [[ ${1:-""} == "start" ]];then shift && nice fork -pf="$0.$serv.pids" --single  "$0 $*"  --wait --status # enable -wait or redirect out to log
elif [[ ${1:-""}  == "stop" ]];then shift && fork -pf="$0.$serv.pids" -kila
else 
# --------- begin of script body ----

table=${1:? full tablename !} # like "rnd600.history_ref"
date_field=${2:?date_field!} # имя поля с датой , например "d"
verbose=${3:-"2"} # уровень вывода 0/1/2. 0-молча. 1-выводить только итоги. 2-отладка
[[ -z "$verbose" ]] && verbose="0"; 
[[ "$verbose" -gt 1 ]] && set -x # выше 1 включает отладку
############################
# Лимит дней. Если в ьаблице больше этого кол-ва, то будем удалять старые дни.
max_days=14 # ПОПРАВИТЬ
#[[ "$verbose" -eq 1 ]] && echo "max_days: $max_days">&2
############################

# сколько дней в table текущего сервера
days_lists_in_table=`khpost "select distinct $date_field from ${table} order by $date_field"` # упорядоченный список дней 
days_count_in_table=`echo "$days_lists_in_table" | wc -l`
if [[ "$days_count_in_table" -gt "$max_days" ]]; then
 # сколько записей сейчас
 records_count=`khpost "select count() from $table"`
 # если дней больше, чем лимит, сколько дней удалить:
 number_of_days_to_delete=$(( $days_count_in_table - $max_days ))
 
 
 set +o pipefail # head и tail не должны приводить к ошибке
   # дни для удаления
   days_list_to_delete=`echo "$days_lists_in_table" | head -n${number_of_days_to_delete}`
 
   [[ -z "$days_list_to_delete" ]] && echo "Empty days_list_to_delete!" && exit 1
   first_day_to_delete=`echo "$days_list_to_delete" | head -n1`
   last_day_to_delete=`echo "$days_list_to_delete" | tail -n1`
   [[ "$verbose" -eq 1 ]] && echo "Delete days from $first_day_to_delete to $last_day_to_delete">&2
 set -o pipefail

 ################# АТАЧ-ДЕТАЧЬ ######
 # ВКЛЮЧИТЬ:
 ./detach_from_attach_to.sh "$table" "$table" "$first_day_to_delete" "$last_day_to_delete"
 ####################################

 # сколько стало дней в базе
 now_days_lists_in_table=`khpost "select distinct d from ${table}"`
 now_days_count_in_table=`echo "$days_lists_in_table" | wc -l`
 
 now_records_count=`khpost "select count() from $table"`
 [[ "$records_count" -eq "$now_records_count" ]] && echo "Nothing records was deleted from $table (now has $now_records_count records )" && exit 1
fi

# --------- end of script bidy ------ 
fi

#)>>"$0.log" 2>&1
