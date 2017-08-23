#!/usr/bin/env bash
#>
#(
set -u
set -x
set -o pipefail
cd `dirname $0`

servers=`cat servers.conf` #"dm22 dm23"
# rnd600 exists? 
for kh_server in $servers; do
 export kh_server
 db="rnd600"
 khpost  -y"show databases" -r"$db" && echo "$kh_server: database $db exists" && continue #exit 0

 khpost -q"CREATE DATABASE rnd600"
done
#)>>"$0.log" 2>&1
