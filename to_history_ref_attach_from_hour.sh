#!/usr/bin/env bash
#> детачит дневную ${main_table}_${day_} таблицу, аттачит к главной ${main_table}, удаляет дневную
#> 
#(
set -u
set -x
set -o pipefail
cd `dirname $0`

export PERL5LIB=${PERL5LIB:-""}:/usr/local/rle/var/share3/TIKETS/bike

# single process!
serv=`hostname`
if [[ ${1:-""} == "start" ]];then shift && nice fork -pf="$0.$serv.pids" --single "$0 $@"  --wait --status # enable -wait or redirect out to log
elif [[ ${1:-""}  == "stop" ]];then shift && fork -pf="$0.$serv.pids" -kila
else 
# --------- begin of script body ----

# Параметры:
hour_file=${1?HOUR!}
hour=`echo $hour_file | fn2hours`

deb=${2:-""}
[[ -n "$deb" ]] && set -x
db="rnd600"
main_table="history_ref"
my_server=`hostname`
myname=$( basename $0 )

h="$hour"
H00=`echo $h | perl -lane'/T(\d\d)/ && print $1'` # like 05
H=`echo "$H00" | perl -lane'print $_+=0'`
day=`echo $hour | fn2days`


hour_table="${main_table}_${h//-/_}" # like: history_ref_2015_05_16T05

# Нельзя допустить чтобы в основную таблицу переаттачатся неполные данные за час.

# Этот скрипт врутри команды --cmd washing. Поэтому файл еще .TMP (не готов)
if [[ ! -s "$hour_file.TMP" && ! -s "$hour_file" ]]; then
 echo "$myname: File $hour_file not exists or empty. Exit 1.">&2
 exit 1
fi 

# перенос данных через attach из часа в основную таблицу


resdir=`dirname $hour_file`
result_file="$resdir/${H00}_reattached_${my_server}.txt" # признак того, что реаттач уже был
[[ -s "$result_file" ]] && echo "$myname: File $result_file already exists. Ok." && exit 0 # если есть этот файл, проходим мимо
hour_=`echo "$hour" | sed -e's|-|_|g'`

# В main_table есть этот час?
khpost -y"select toHour(sec) from $db.$main_table where d='$day' and toHour(sec)=$H limit 1" -r'\d+' && exit 0 # выходим: день есть в main_table

if ! khpost -y"EXISTS TABLE $db.${main_table}_${hour_}" ; then
    echo "$myname $my_server: table $db.${main_table}_${hour_} not exists. Exit 1."
    exit 1 # таблицы нет
fi
    
# проверка наличия данных часа в часовой таблице (полнота часов не проверяется):
if ! khpost -y"select toHour(sec) from $db.${main_table}_${hour_} limit 1" -r'\d+' ; then
    echo "$myname $my_server: empty table $db.${main_table}_${hour_}. Exit 1.">&2
    exit 1
fi    
    
    
if ! ./detach_from_attach_to.sh "$db.${main_table}_${hour_}" "$db.$main_table" >> $result_file ; then
    echo "$myname $my_server: detach from $db.${main_table}_${hour_} returns NOT OK. exit 1.">&2
    exit 1
fi
    
# проверка на пусто и удаление таблицы:
khpost -y"select count() from $db.${main_table}_${hour_}" && 
	echo "$myname $my_server: Table $db.${main_table}_${hour_} is not empty after detach. Exit 1.">&2 && exit 1
    
khpost "DROP TABLE $db.${main_table}_${hour_}" --FORCE
    
#echo "$my_server: $db.${main_table}_${hour_} attached to $db.$main_table">>$result_file


#---

# --------- end of script bidy ------ 
fi

#)>>"$0.log" 2>&1
