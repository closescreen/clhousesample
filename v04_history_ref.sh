set -x
set -u
set -o pipefail

rollback=${1:-""}

servers=`cat servers.conf`

for kh_server in $servers; do

 export kh_server #=dm22
 t=rnd600.history_ref

 #dom - dowWithoutWWW( ref )
 #ключ: sid,dom,sz +uid
 #сэмплирование по uid
 #добавлен URLHash(ref)
 #добавлены floor,ddid

 if [[ "$rollback" == "rollback" ]];then
  khpost "drop table $t"
 else 


 khpost -q"
 CREATE TABLE rnd600.history_ref
    (
    uid UInt64,
    sec DateTime,
    expid UInt64,
    tn Int8,
    sn Int8,
    stn Int8,
    ref String,
    dom String MATERIALIZED domainWithoutWWW(ref),
    refhash String MATERIALIZED URLHash(ref),
    sid UInt64,
    sz UInt32,
    bref String,
    geo UInt16,
    ipint UInt32,
    ip String ALIAS IPv4NumToString(ipint),
    pz Int32,
    bt Int32,
    ad Int64,
    net Int64,
    exppr Float64,
    winexppr Float64,
    secondpr Float64,
    floor Float64,
    ddid Int64,
    custom String,
    d Date MATERIALIZED toDate(sec)
  ) ENGINE = MergeTree(d, uid, (sid, dom, sz, uid), 8192)
"

 fi

done
