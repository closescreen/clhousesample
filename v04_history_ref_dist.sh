set -x
set -u
set -o pipefail

rollback=${1:-""}

servers=`cat servers.conf`

for kh_server in $servers; do

 export kh_server #=dm22
 db="rnd600"
 table="history_ref_dist"
 look_to_table="history_ref"
 cluster="dms"

 # distributed таблица смотрящая на history_ref 

 if [[ "$rollback" == "rollback" ]];then
    khpost "DROP TABLE $db.$table"
 else 
    khpost -q"CREATE TABLE $db.$table AS $db.$look_to_table ENGINE = Distributed( $cluster, $db, $look_to_table)"
 fi

done
