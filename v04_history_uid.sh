
set -u
set -o pipefail
set -x

servers=`cat servers.conf`

for kh_server in $servers; do

 export kh_server

 t="rnd600.history_uid"

 #Только sn==0
 #Только tn!=2
 #ref и bref заполняются в зависимости от sid входит в справочник
 #ip_bid - ip из строки бида,  аналогично ip_exp, ip_cl 
 #dom alias

 rollback=${1:-""}
 if [[ "$rollback" == "rollback" ]];then
  khpost "DROP TABLE rnd600.history_uid"
 else

 khpost -d "CREATE TABLE rnd600.history_uid 
    ( /* Заливаем только sn==0 */
    uid UInt64, 
    expid UInt64, /* expid для бид-показ-клик искать в окне 2 часа */
    tn Int8, /* кроме tn==2 */ 
    sec DateTime,
    delta30 Int32 DEFAULT 0, /*seconds between bid and exp*/
    delta01 Int32 DEFAULT 0, /*seconds betweeb exp and click*/
    ref String, /* в списке ssp ? ref из строки bid : ref из строки exp */
    dom ALIAS domainWithoutWWW(ref),
    sid UInt64, 
    sz UInt32, 
    bref String, /* в списке ssp ? bref из строки bid : ref из строки exp */
    geo UInt16,
    ipint_bid UInt32, /* из строки бида*/
    ipint_exp UInt32, /* из строки показа */
    ipint_cl  UInt32, /* из строки клика */
    bt Int32,

    ip_bid String ALIAS ipint_bid==0 ? '' : IPv4NumToString(ipint_bid),
    ip_exp String ALIAS ipint_exp==0 ? '' : IPv4NumToString(ipint_exp),
    ip_cl String  ALIAS ipint_cl==0 ? '' : IPv4NumToString(ipint_cl),

    has_bid UInt8 ALIAS ipint_bid ==0 ? 0: 1,
    has_exp UInt8 ALIAS ipint_exp ==0 ? 0: 1,
    has_cl  UInt8 ALIAS ipint_cl ==0 ? 0: 1,

    d Date MATERIALIZED toDate(sec)
  ) ENGINE = MergeTree(d, uid, uid, 8192)
 "

 fi
done

