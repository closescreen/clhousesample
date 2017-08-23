#!/usr/bin/env bash
#> Заливка в <insert_table>

#> На STDIN подать данные 
#(
set -u
set -x
set -o pipefail
cd `dirname $0`


f=${1:? FILE! } # File with <hour> inside. like: "../../reg_history_ref/2017-05-15/04.txt"
usergroup_from=${2:? usergroup_from!} # 
usergroup_to=${3:? usergroup_to!} # 
deb=${4:-""}
#[[ -n "$deb" ]] && set -x
myname=$( basename $0 )
my_server=`hostname`


hour=`echo $f | fn2hours`
hour_=${hour//-/_}

day=`echo $hour | fn2days` 
H=`echo $hour | perl -lane'/T(\d\d)/ && print $1'` # 05
H=`echo "$H" | perl -lane'print $_+=0'`

db="rnd600"
main_table="history_ref"
insert_table="$db.${main_table}_${hour_}" # Table for insert data into. like: rnd600.history_ref_2017_05_16

if khpost -y"select toHour(sec) from $db.$main_table where d='$day' and toHour(sec)=$H limit 1" -r'\d+' ; then
    # выходим:  есть в main_table
    [[ -n "$deb" ]] && echo "$myname $my_server: day:$day hour:$H exists in $db.$main_table. Ok. ">&2
    exit 0
fi

[[ -n "$deb" ]] && echo "$myname $my_server: day:$day hour:$H Start ./history_ref_from_history_v04.py "$f" "$usergroup_from" "$usergroup_to" ">&2

python ./history_ref_from_history_v04.py "$f" "$usergroup_from" "$usergroup_to"  |
 clickhouse-client --receive_timeout=600 --send_timeout=600 --connect_timeout=600 --query="INSERT INTO ${insert_table} format TSV" || 
  exit 1

echo "OK. $insert_table" # для сохранения в файлах reg_history_ref хоть чего-нибудь



#)>>"$0.log" 2>&1
