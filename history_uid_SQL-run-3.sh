set +x
set -o pipefail
set -u

# Для каждого файла из sqldir не имеющего рядом "$f.ok.result" выполняет SQL из файла и создает "$f.ok.result"
# SQL- файлы общие для всех серверов.
# Файлы результатов, логов - для каждого сервера - свои.
# Однопоточный.

myserv=`hostname`

day=${1:?DAY!} #"2017-03-20"
sqldir="../../reg_history_uid/$day"
for f in `find $sqldir -name "*.sql"`; do
    #echo "$f..."
    [[ -s "$f.${myserv}.ok.result" ]] && continue # если есть .ok. то пропуск
    t1=`date +%s`
    cat "$f" | clickhouse-client >>"$f.${myserv}.log" 2>&1
    if [[ $? -eq 0 ]]; then echo ok>>$f.${myserv}.ok.result; else echo NO>>$f.${myserv}.no.result; fi
    t2=`date +%s`
    dt=$(( $t2 - $t1 ))
    sl=$(( $dt / 10 )) 
    sleep $sl
done

# потом еще нужна будет очистка от старых файлов
