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
    echo "$myname $my_server: Drop table $db.${main_table}_${hour_} for recreate">&2
    khpost "DROP table if exists $db.${main_table}_${hour_}" --FORCE
fi

if ! khpost -q"CREATE TABLE IF NOT EXISTS $db.${main_table}_${hour_} AS $db.$main_table"; then
    echo "$myname $my_server: Can't create $db.${main_table}_${hour_}">&2
    exit 1
fi