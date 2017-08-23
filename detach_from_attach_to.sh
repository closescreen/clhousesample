#!/usr/bin/env bash



#> из буферной таблицы (buff_full_name) приатачивает данные к главной (main_full_name)
#> имя буферной таблицы может быть равно главной, значит данные переаттачиваются у одной таблицы
#> в случае одной таблицы, нужно указать min и max дни, которые нужно вырезать, данные за которые не приатачивать обратно
#> вырезаются (т.е. не атачатся обратно) куски таблиц, которые затрагивают этот диапазон дней, но НЕ ЗАТРАГИВАЮТ другие дни.
#> Могут остаться данные содержащие эти дни из-за того, что кусок данных затрагивает и другие дни.

#(
set -u
set +x
set -o pipefail
cd `dirname $0`

# рассчитан на только один процесс в один момент времени

# варианты использования:
# 1. ./script <buff_full_name> <main_full_name>  -- перенести все из buff_full_name в main_full_name:
# 2. ./script <buff_full_name> <main_full_name> day1 day2 -- за исключением кусков за day1..day2 (для удаления данных за день) 
#   примечание к 2: <buff_full_name> может быть равна <main_full_name> - данные детачатся и аттачатся обратно за исключением дней
# 3. Когда в папках detached/ найдены файлы, скрипт останавливается. Нужно сначала
#     или переместить их в detached/to_remove/:
#     ./script <buff_full_name> <main_full_name> REMOVE_DETACHED
#     или приатачить к главной ():
#     ./script <buff_full_name> <main_full_name> ATTACH_DETACHED
#  а потом запустить с обычными параметрами. 

data_dir="/usr/local/rle/var/clickhouse/data"

# параметры:
# полное имя "буферной" таблицы (из которой переносятся двнные):
buff_full_name=${1:? buffer table full name !} # like: 'rnd600.history_ref_2017_05_16'
# полное имя "главной" таблицы (в которую переносятся данные):
main_full_name=${2:? main table full name !} # like: 'rnd600.history_ref'

# может быть действие, может быть день
# действие - одна из перечисленных строк, что делать с существующими файлами в папках detached/ (когда они есть там)
# либо день - значение для except_min_day, см. ниже.
action=${3:-""} # nothing/""  | "REMOVE_DETACHED" | "ATTACH_DETACHED"

if [[ $action =~ [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] ]];then
 except_min_day=${3:-""} # день ОТ, включительно, который не приаттачивать после детача (подлежит удадению)
 except_max_day=${4:-""} # день ДО, включительно, который не приаттачивать после детача
 action=""
else
 except_min_day=${4:-""} 
 except_max_day=${5:-""} 
fi

[[ -z "$action" || "$action" == "REMOVE_DETACHED" || "$action" == "ATTACH_DETACHED" ]] || 
    ( echo "action may be one of: ''|REMOVE_DETACHED|ATTACH_DETACHED">&2 && exit 1 ) 


# если не указан max day он принимается равним min day:
[[ -z "$except_max_day" ]] && except_max_day="$except_min_day"

# формат дней д.б. yyyy-mm-dd:
if [[ -n "$except_min_day" ]]; then 
 [[ $except_min_day =~ [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] ]] || (echo "min day - bad format! yyyy-mm-dd">&2 && exit 1)
fi 
if [[ -n "$except_max_day" ]];then 
 [[ $except_max_day =~ [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] ]] || (echo "max day - bad format! yyyy-mm-dd">&2 && exit 1)
fi

# Если имя таблиц совпадает то д.б. указаны дни: 
[[ "$buff_full_name" == "$main_full_name" && -z "$action" && -z "$except_min_day" && -z "$except_max_day" ]] && 
 echo "Either different tables or set days!">&2 && exit 1

# имена баз данных
buff_db=${buff_full_name//\.*/} # like rnd600
main_db=${main_full_name//\.*/} # like rnd600 ( may differ )

# короткие имена таблиц
buff_short_name=${buff_full_name//*\./} # like history_ref_2017_05_16
main_short_name=${main_full_name//*\./} # like history_ref_2017_05_16

# detached-папки таблиц
buff_detached_path="$data_dir/$buff_db/$buff_short_name/detached"
main_detached_path="$data_dir/$main_db/$main_short_name/detached"

# Начальное количество записей в главной таблице:
main_count_before=`khpost --int "select count() from $main_full_name"` || exit 1 # выход при ошибке kh

# Начальное количество записей в буферной таблице:
if [[ "$main_full_name" == "$buff_full_name" ]];then
 buff_count_before=$main_count_before # если это одна и таже таблица
else
 buff_count_before=`khpost --int "select count() from $buff_full_name"` || exit 1 # выход при ошибке kh
fi
[[ "$buff_count_before" -eq 0 ]] && echo "$buff_full_name is empty.">&2

# количество detached файлов было у main таблицы:
was_main_detached_files=`find "$main_detached_path/" -mindepth 1 -maxdepth 1 -type d -name "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*"`
if [[ -z "$was_main_detached_files" ]];then
 was_main_detached_count=0
else
 was_main_detached_count=`echo "$was_main_detached_files" | wc -l` || exit 1
fi 

# предупреждение, если что-то было detached у main:
if [[ "$was_main_detached_count" -ne 0 ]]; then
 echo "Detached files in $main_detached_path/. count: $was_main_detached_count">&2
 if [[ "$action" == "REMOVE_DETACHED" ]]; then
  mv $was_main_detached_files -t "$main_detached_path/to_remove/" || exit 1
  echo "Moved to $main_detached_path/to_remove/. Run script again for normal continue.">&2 && exit 1
  exit 0
 elif [[ -z "$action" ]];then
  echo "Use REMOVE_DETACHED or ATTACH_DETACHED. ">&2 && exit 1
 fi
fi

if [[ "$main_full_name" != "$buff_full_name" ]];then
 # количество detached файлов было у buff таблицы:
 was_buff_detached_files=`find $buff_detached_path/ -mindepth 1 -maxdepth 1 -type d -name "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*"` || exit 1
 if [[ -z "$was_buff_detached_files" ]];then
  was_buff_detached_count=0
 else
  was_buff_detached_count=`echo "$was_buff_detached_files" | wc -l` || exit 1
 fi
  
 # предупреждение если что-то было detached:
 if [[ "$was_buff_detached_count" -ne 0 ]];then
  echo "Detached files found in $buff_detached_path/. count: $was_buff_detached_count">&2
  if [[ "$action" == "REMOVE_DETACHED" ]]; then
   mv $was_buff_detached_files -t "$buff_detached_path/to_remove/" || exit 1
   echo "Momoved to $buff_detached_path/to_remove/. Run script again for normal continue.">&2 && exit 1
   exit 0

  elif [[ -z "$action" ]];then
   echo "Use REMOVE_DETACHED or ATTACH_DETACHED for continue.">&2 && exit 1
  fi
 fi
else
 was_buff_detached_files="$was_main_detached_files"
 was_buff_detached_count="$was_main_detached_count"
fi

# Из day min и max - список дней включительно:
except_days=""
if [[ -n "$except_min_day" ]]; then
 except_days=`hours -t="${except_min_day}T01" -tot="${except_max_day}T02" -days`
fi

# из списка дней - список уникальных затрагиваемых partition ч. запятую (в формате yyyymm). Пусто если нет дней. :
days_partitions_comma=`echo $except_days | perl -lane"/(\\d{4})-(\\d\\d)/ and \\$rv{\"'\\$1\\$2'\"}=1  for @F; END{ print join',',keys %rv}"`

# имена затрагиваемых для detach partitions (yyyymm) в буферной таблице:
if [[ -n "$days_partitions_comma" ]]; then
 buff_partitions=`khpost "SELECT distinct partition p FROM system.parts WHERE active and database='$buff_db' and table='$buff_short_name' 
  and p in ($days_partitions_comma)"` || exit 1
else
 buff_partitions=`khpost "SELECT distinct partition FROM system.parts WHERE active and database='$buff_db' and table='$buff_short_name'"` || exit 1
fi

# список (для detach) partitions через запятую:
buff_partitions_comma=`echo $buff_partitions | perl -lane"\\$rv{\"'\\$_'\"}=1 for @F; END{ print join',',keys %rv}"`

# Аттачить будем эти имена затрагиваемых кусков в буферной таблице:
if [[ -n "$buff_partitions_comma" ]]; then 
 if [[ -n "$except_days" ]]; then
  # только затрагиваемых partitions за исключением дней min .. max:
  buff_parts=`khpost "SELECT name FROM system.parts WHERE active and database='$buff_db' and table='$buff_short_name' 
   and partition in ($buff_partitions_comma) and not (min_date>='$except_min_day' and max_date<='$except_max_day')"` || exit 1
 else
  # только затрагиваемых partitions:
  buff_parts=`khpost "SELECT name FROM system.parts WHERE active and database='$buff_db' and table='$buff_short_name' 
   and partition in ($buff_partitions_comma)"` || exit 1
 fi  
else
 # всех partitions данной таблицы:
 buff_parts=`khpost "SELECT name FROM system.parts WHERE active and database='$buff_db' and table='$buff_short_name'"` || exit 1
fi

# если дни были указаны, но в исключении из нет, такое м.б. когда были только детаченые ранее файлы:
[[ -n "$except_min_day" && -z $except_days ]] && echo "Days '$except_min_day' and '$except_max_day' not affect for attach files.">&2

# Детач partitions:
if [[ "$was_main_detached_count" -eq 0 && "$was_buff_detached_count" -eq 0 ]]; then 
 # (если в detached папках чисто)
 for p in $buff_partitions; do
  khpost "ALTER TABLE $buff_full_name DETACH PARTITION $p" --FORCE || exit 1
 done

 # else:
 # если в detached не было чисто, не детачим - пропускаем это, переходим к этапу аттача 
 # полагая что в прошлый раз что-то сбойнуло
 # здесь может располагаться код, по удалению/перемещению данных если мы решили не доаттачивать остатки с прошлого раза
fi

# если было (раньше) в главной detached папке чисто:
if [[ "$was_main_detached_count" -eq 0 ]]; then 
 # перенесение содержимого папки buff detached --> в main detached:
 if [[ "$main_full_name" != "$buff_full_name" ]]; then
  # если main и buff - это разные таблицы:
  # файлы находящиеся сейчас в buff detached: 
  buff_detached_files=`find $buff_detached_path/ -mindepth 1 -maxdepth 1 -type d -name "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*"` || exit 1
  [[ -z "$buff_detached_files" ]] && echo "No detached files in $buff_detached_path/">&2 && exit 1
  mv $buff_detached_path/* -t $main_detached_path/ || exit 1

  # else:
  # если buff и main - это одна и таже таблица, то ничего не переносим
 fi 
fi 

# должны появиться файлы в main detach:
main_files_now=`find "$main_detached_path/" -mindepth 1 -maxdepth 1 -type d -name "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*"` || exit 1
[[ -z "$main_files_now" ]] && echo "Nothing to attach: not files in $main_detached_path/">&2 && exit 1

if [[ -n "$was_main_detached_files" || -n "$was_buff_detached_files" ]];then
 if [[ "$action" == "ATTACH_DETACHED" ]];then
  # пустой список buff_parts может быть если данные уже были детачены.
  echo "Determining parts from filenames from $main_detached_path/">&2
  # если буферная табл была пуста, найдем куски путем чтения имен папок в detach главной таблицы:
  buff_parts=`echo "$main_files_now" | perl -lape's|^.+/||g'`
  echo "buff_parts: $buff_parts">&2
 else
  echo "Use 'ATTACH_DETACHED' for continue.">&2 && exit 1
 fi
 # если нет ничего то выход:
 [[ -z "$buff_parts" ]] && echo "Not found parts in $main_detached_path">&2 && exit 1
fi

# аттач данных к main_table:
for p in $buff_parts; do
 khpost "ALTER TABLE $main_full_name attach part '$p'" --FORCE || exit 1
done


# кол-во записей в main_table после добавления (должно быть не меньше чем сумма main_count_before + day_count_before):
main_count_after=`khpost --int "select count() from $main_full_name"` || exit 1

if [[ "$main_full_name" != "$buff_full_name" ]];then
 # если из одной таблицы в другую, то д.б. сумма кол-ва записей:
 expected_main_count=$(( $main_count_before + $buff_count_before ))
 if [[ "$main_count_after" -eq "$expected_main_count" ]]; then
  echo "attached_records: $buff_count_before; partitions: $buff_partitions; from: $buff_full_name; to: $main_full_name"
 else 
  echo "After attach partitions ($buff_partitions) count of record in $main_full_name NOT EQUAL to expected.
     in main table before: $main_count_before + 
     in buff table before: $buff_count_before 
     expected in main: $expected_main_count
     really in main: $main_count_after">&2 && exit 1
 fi

else
 # 1
 # если из одной и той же таблицы детач-аттач:
 # нужно знать количество выкинутых записей ...
 
 # 2
 # нужно что-то сделать с детаченными кусками.
 # что есть в buff detached:
 to_left_detached_parts=`find "$buff_detached_path/" -mindepth 1 -maxdepth 1 -type d -name "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*"`
 if [[ -n "$to_left_detached_parts" ]]; then # если что-то есть
  # создать папку to_remove:
  mkdir -p "$buff_detached_path/to_remove/" || exit 1
  # если в to_remove уже что-то есть - удалить:
  already_removed=`find "$buff_detached_path/to_remove/" -mindepth 1 -maxdepth 1 -type d`
  if [[ -n "$already_removed" ]];then
   echo "Remove $already_removed">&2
   rm -rf $already_removed || exit 1
  fi 
  # переместить детаченные из buff detached в buff detached to_remove:
  mv $to_left_detached_parts -t "$buff_detached_path/to_remove/"
 fi
fi



#)>>"$0.log" 2>&1
