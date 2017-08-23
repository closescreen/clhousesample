set +x
set -o pipefail
set -u

db="rnd600"
main_table="history_ref"
hour_file=${1? hour file ! }
hour=`echo $hour_file | fn2hours`
hour_=${hour//-/_}
my_server=`hostname`
myname=$( basename $0 )

if khpost -y"exists table $db.${main_table}_${hour_}"; then
    #echo "DROP $db.${main_table}_${hour_} after insert">&2
    if ! khpost "DROP table if exists $db.${main_table}_${hour_}" --FORCE; then
	echo "$myname $my_server: Can't drop $db.${main_table}_${hour_}">&2
	exit 1
    fi
fi 

#echo "$db.${main_table}_${hour_} dropped">&2
