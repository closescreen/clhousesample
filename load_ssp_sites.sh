#!/usr/bin/env bash
#>
#(
set -u
set -x
set -o pipefail
cd `dirname $0`


serv=`hostname`
if [[ ${1:-""} == "start" ]];then shift && nice fork -pf="$0.$serv.pids" --single "$0 $@"  --wait --status # enable -wait or redirect out to log
elif [[ ${1:-""}  == "stop" ]];then shift && fork -pf="$0.$serv.pids" -kila
else 
# --------- begin of script body ----

filename=/usr/local/rle/var/share3/DATA/dicts/ssp_sites.txt


#cat "$filename" | perl -lape's|\t+|\t|g' | kh.insert --fields="sid, name" --into="rnd600.ssp_sites" --viatmp --addr="$kh" --tmpsuff="1" --deb
t="rnd600.ssp_sites"
tmp_t="${t}_TMP"

khpost -q"DROP TABLE IF EXISTS $tmp_t" --FORCE &&
 khpost -q"CREATE TABLE $tmp_t AS $t" &&
 ( cat "$filename" | perl -lape's|\t+|\t|g' | clickhouse-client -q"INSERT INTO $tmp_t (sid, name) FORMAT TSV" ) &&
	khpost -q"INSERT INTO $t SELECT * FROM $tmp_t" &&
	    khpost -q"DROP TABLE $tmp_t" --FORCE

# --------- end of script bidy ------ 
fi

#)>>"$0.log" 2>&1
