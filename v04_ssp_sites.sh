set -o pipefail
set -x
set -u

#export kh_server=dm22
t="rnd600.ssp_sites"

servers=`cat servers.conf`

for kh_server in $servers; do

 export kh_server #=dm22


 khpost -d  -q"
  CREATE TABLE rnd600.ssp_sites 
    ( 
    sid UInt64,  
    name String,  
    d Date DEFAULT today()
    ) ENGINE = MergeTree(d, sid, 8192)
 "
done

#"DROP TABLE rnd600.ssp_sites")|>

