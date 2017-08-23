
set -u
set -o pipefail
set -x

servers=`cat servers.conf`

for kh_server in $servers; do

 export kh_server

 db="rnd600"
 table="history_uid_dist"
 look_to_table="history_uid"
 cluster="dms"


 #Только sn==0
 #Только tn!=2
 #ref и bref заполняются в зависимости от sid входит в справочник
 #ip_bid - ip из строки бида,  аналогично ip_exp, ip_cl 
 #dom alias

 rollback=${1:-""}
 if [[ "$rollback" == "rollback" ]];then
  khpost "DROP TABLE $db.$table"
 else

 khpost -d "CREATE TABLE $db.$table as $db.$look_to_table  ENGINE = Distributed( $cluster, $db, $look_to_table)
 "

 fi
done

