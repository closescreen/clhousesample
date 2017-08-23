#!/usr/bin/env bash
#> создает SQL-запросы для переливки из history_ref в history_uid
#> Печатает на STDOUT
#> USAGE: ./history_uid_SQL-generate.sh | viatmp history_uid_SQL.txt ... | history_uid_SQL-run-2.sh
# или > history_uid_SQL.txt
#(
set -u
set +x
set -o pipefail
cd `dirname $0`

N=1 # количество процессов

serv=`hostname`
if [[ ${1:-""} == "start" ]];then shift && nice fork -pf="$0.$serv.pids" -n="$N" -dela=3 -ed=s "$0 $@"  --wait --status # enable -wait or redirect out to log
elif [[ ${1:-""}  == "stop" ]];then shift && fork -pf="$0.$serv.pids" -kila
else 
# --------- begin of script body ----

# для каждого количества, не превышающего предела, будем делать запрос на переливку
# ssp и не ssp - отдельно

day=${1:?DAY!} #"2017-03-20"
nextday=`hours -d="$day" -n=2days -days --last`


# проверка наличия 24 часов в history_ref:
table_from=rnd600.history_ref
hours_in_table_found=`khpost --int "select uniq(toHour(sec)) from $table_from WHERE d=='$day'"`
[[ "$hours_in_table_found" -ne 24 ]] && echo "Only $hours_in_table_found hours in table ${table_from} for day ${day} found">&2 && exit 1


# кол-ва по sid для не ssp:
clickhouse-client -q"SELECT count() c1, sid FROM rnd600.history_ref 
 WHERE d=='$day' AND sn==0 AND expid!=0 AND sid NOT IN (SELECT sid from rnd600.ssp_sites) group by sid order by sid" |
 perl -Mstrict -e'
my $day = shift() or die "DAY!";
my $nextday = shift() or die "NEXTDAY!";
my $maxlimit = 10_000_000;
my $cntsum;
my %sids;
my ( $begin, $exp_end, $cl_end );
my @intervals = (
    { begin => "${day}T00:00:00", exp_end => "${day}T05:59:59", click_end => "${day}T07:59:59" },
    { begin => "${day}T06:00:00", exp_end => "${day}T09:59:59", click_end => "${day}T11:59:59" },    
    { begin => "${day}T10:00:00", exp_end => "${day}T13:59:59", click_end => "${day}T15:59:59" },
    { begin => "${day}T14:00:00", exp_end => "${day}T16:59:59", click_end => "${day}T18:59:59" },
    { begin => "${day}T17:00:00", exp_end => "${day}T19:59:59", click_end => "${day}T21:59:59" },
    { begin => "${day}T20:00:00", exp_end => "${day}T21:59:59", click_end => "${day}T23:59:59" },
    { begin => "${day}T22:00:00", exp_end => "${day}T23:59:59", click_end => "${nextday}T01:59:59" },
);

while(<STDIN>){
 my ($cnt, $sid) = split;

 # суммируем cnt до тех пор пока лимит не будет превышен
 $cntsum+=$cnt; $sids{$sid}||=1;
 #warn "cntsum=$cntsum, cnt=$cnt, sid=$sid ";
 
 if ( $cntsum > $maxlimit ){
    # получить SQL-текст no_ssp для @sids
    my $rv = for_each_interval( \@intervals, \%sids);
    for my $text (@$rv){
	print $text, "\n\n"; # разделены пустой строкой
    }
    $cntsum = 0; %sids = ();
 }
}
if (%sids){
    my $rv = for_each_interval( \@intervals, \%sids);
    for my $text (@$rv){
	print $text, "\n\n"; # разделены пустой строкой
    }
    $cntsum = 0; %sids = ();
 } 


sub for_each_interval{
 my @intervals = @{ shift() } or die "intervals!";
 my %sids = %{ shift() } or die "sids!";
 my @rv;
 for my $interval (@intervals){
        $begin = $interval->{begin};
        $exp_end = $interval->{exp_end};
        $cl_end = $interval->{click_end};
	
	my @sids = sort {$a<=>$b} keys %sids;	
	my $comment = "/*cntsum=$cntsum, sids:@sids, ($begin -> $exp_end, $cl_end)*/\n";
        my $text = sql_text( \@sids, $begin, $exp_end, $cl_end ) or die "Cant get sql_text for nossp sids:@sids";
	
	push @rv, $comment.$text;
 }
 return \@rv;
}

sub sql_text{
 local $" = ",";
 my @sids = @{ shift() } or die "no ssp: SIDS!";
 my $begin = shift or die "sec begin!"; # like 2017-03-20T20:00:00
 my $exp_end = shift or die "sec exp_end!";
 my $click_end = shift or die "sec click_end!";

 my $q="'"'"'"; # это одиночная кавычка
 "INSERT INTO rnd600.history_uid
  SELECT toUInt64(auid) uid, aexpid expid, atn tn, asec sec, adelta30 delta30, adelta01 delta01, aref ref, asid sid, asz sz, 
    abref bref, ageo geo, aipint_bid ipint_bid, aipint_exp ipint_exp, aipint_cl ipint_cl, abt bt 
  FROM (
   SELECT 
    any(uid) auid,
    any(expid) aexpid, 
    toInt8( tn0ind==0? -1: 0 ) atn, /*если нет показа, то tn=-1 подстраховка от ошибок*/
    tn0ind>0 ? secs[tn0ind] : secs[1] asec, /*нельзя допускать нули*/
    toInt32(0) adelta30, 
    ( tn0ind>0 and tn1ind>0 )? (secs[tn1ind]-secs[tn0ind]) : 0 as adelta01, 
    groupArray(ref)[tn0ind] aref, /*не ssp - ref показа*/
    toUInt64( any(sid)) asid,
    any(sz) asz,
    groupArray(bref)[tn0ind] abref,
    any(geo) ageo,
    toUInt32(0) aipint_bid,
    groupArray(ipint)[tn0ind] aipint_exp,
    groupArray(ipint)[tn1ind] aipint_cl,
    any(bt) abt,
    groupArray(tn) tns, 
    indexOf(tns,0) tn0ind, 
    indexOf(tns,1) tn1ind,
    groupArray(sec) secs
   FROM ( /*не SSP показы */ 
    SELECT uid, sec, expid, tn, sid, sz, bt, geo, ref, bref, ipint
    FROM rnd600.history_ref WHERE sec BETWEEN ${q}${begin}${q} AND ${q}${exp_end}${q} AND sid IN (@sids)
    AND tn==0 AND sn==0 AND expid!=0
   UNION ALL /*не SSP клики */
    SELECT uid, sec, expid, tn, sid, sz, bt, geo, ${q}${q} ref , ${q}${q} bref, ipint 
    FROM rnd600.history_ref WHERE sec BETWEEN ${q}${begin}${q} AND ${q}${click_end}${q} AND sid IN (@sids)
    AND tn==1 AND sn==0 AND expid!=0
   ) 
   GROUP BY expid 
   HAVING tn0ind!=0 /*клики без показов не нужны*/
  )
  "
}

' "$day" "$nextday"

# ================== SSP =========================================================================================

clickhouse-client -q"SELECT count() c1, sid, sz FROM rnd600.history_ref 
 WHERE d=='$day' AND sn==0 AND expid!=0 AND sid IN (SELECT sid from rnd600.ssp_sites) group by sid,sz order by sid,sz" |
 perl -Mstrict -e'
my $day = shift() or die "DAY!";
my $nextday = shift() or die "NEXTDAY!";
my $maxlimit = 10_000_000;
my $sz_limit = 5_000; # не превышать количество уникальных сайтзон в SQL-запросе


my %sidszs;  # 187537=>{ 4=>1, 5=>1, 10=>1, ... }
my ( $begin, $bid_end, $exp_end, $cl_end);
my @intervals = (
    { begin => "${day}T00:00:00", bid_end => "${day}T05:59:59", exp_end => "${day}T06:01:59", click_end => "${day}T07:59:59" },
    { begin => "${day}T06:00:00", bid_end => "${day}T11:59:59", exp_end => "${day}T12:01:59", click_end => "${day}T13:59:59" },
    { begin => "${day}T12:00:00", bid_end => "${day}T14:59:59", exp_end => "${day}T15:01:59", click_end => "${day}T16:59:59" },
    { begin => "${day}T15:00:00", bid_end => "${day}T17:59:59", exp_end => "${day}T18:01:59", click_end => "${day}T19:59:59" },
    { begin => "${day}T18:00:00", bid_end => "${day}T20:59:59", exp_end => "${day}T21:01:59", click_end => "${day}T22:59:59" },    
    { begin => "${day}T21:00:00", bid_end => "${day}T23:59:59", exp_end => "${nextday}T00:01:59", click_end => "${nextday}T01:59:59" },

);

my %szs_count; # здесь будем считать уникальные сайтзоны
my $cntsum; # здесь считаем количества записей
while(<STDIN>){
 my ($cnt, $sid, $sz) = split;

 # суммируем cnt до тех пор пока лимит не будет превышен
 $cntsum+=$cnt; $sidszs{ $sid }{ $sz }||=1; 
 
 # отслеживаем количество сайтзон в запросе:
 $szs_count{ $sz }||=1;

 if ( $cntsum > $maxlimit or %szs_count > $sz_limit ){
    # выполнить переливку ssp для %sidszs
    my $rv = for_each_interval( \@intervals, \%sidszs );
    for my $text ( @$rv ){
	print $text, "\n\n"; # разделены пустой строкой
    }
    
    $cntsum = 0; %sidszs = (); %szs_count = ();
 }
}

if (%sidszs){
    my $rv = for_each_interval( \@intervals, \%sidszs );
    for my $text ( @$rv ){
	print $text, "\n\n"; # разделены пустой строкой
    }
}

sub for_each_interval{
 my @intervals = @{ shift() } or die "intervals!";
 %sidszs = %{ shift() } or die "sidszs!";
 my @rv;
 for my $interval (@intervals){
	$begin = $interval->{begin};
	$bid_end = $interval->{bid_end};
	$exp_end = $interval->{exp_end};
	$cl_end = $interval->{click_end};
        
        my @sids = sort {$a<=>$b} keys %sidszs;
        my $comment = "/*cntsum=$cntsum; sids:@sids; (szs - not explained) ($begin -> $bid_end, $exp_end, $cl_end)*/\n";
        
        my $text = $comment . sql_text( \%sidszs, $begin, $bid_end, $exp_end, $cl_end ) or die "Cant get sql_text for SSP begin: sids:@sids";
        push @rv, $text;
 }
 return \@rv;
}

# sql_text для ssp:
sub sql_text{
 local $" = ","; 
 my %sidszs = %{ shift() } or die "SIDSZS!";
 my $begin = shift or die "sec begin!"; # like 2017-03-20T20:00:00
 my $bid_end = shift or die "sec bid_end!"; 
 my $exp_end = shift or die "sec exp_end!";
 my $click_end = shift or die "sec click_end!";

 # часть предложения where про sid, sz
 my @where_sid_sz;
 my @sids = sort {$a<=>$b} keys %sidszs;
 
 for my $i (0..$#sids){
    my $sid = $sids[$i];

    if ( $i == 0 or $i== $#sids ){
	# сайтзоны в начале и в конце отсортированного по sid списка
	# могут быть неполным началом или неполным окончанием списка сайтзон:  sid==... AND sz IN (...неполный список...)
	# Поэтому для этих sid явно перечисляем sz
	my @szs = sort {$a<=>$b} keys %{ $sidszs{ $sid } };
	push @where_sid_sz, " ( sid==$sid AND sz IN (@szs)) ";
    }else{
	# Те sz которые в середине списка - они включают весь возможный список сайтзон, и перечислять все sz - лишняя трата ресурсов
        push @where_sid_sz, " sid==$sid ";
    }
 }
 my $where_sid_sz = " ( " . join( "OR", @where_sid_sz) . " ) ";

 my $q="'"'"'"; # это одиночная кавычка
 "INSERT INTO rnd600.history_uid
  SELECT toUInt64(auid) uid, aexpid expid, atn tn, asec sec, adelta30 delta30, adelta01 delta01, aref ref, asid sid, asz sz, 
    abref bref, ageo geo, aipint_bid ipint_bid, aipint_exp ipint_exp, aipint_cl ipint_cl, abt bt 
  FROM (
   SELECT 
    any(uid) auid,
    any(expid) aexpid, 
    toInt8( tn3ind==0? -1: 3 ) atn, /*если нет бида, то tn=-1 подстраховка от ошибок*/
    tn3ind>0 ? secs[tn3ind] : secs[1] asec, /*нельзя допускать нули*/
    ( tn3ind>0 and tn0ind>0 )? (secs[tn0ind]-secs[tn3ind]) : 0 as adelta30,
    ( tn0ind>0 and tn1ind>0 )? (secs[tn1ind]-secs[tn0ind]) : 0 as adelta01, 
    groupArray(ref)[tn3ind] aref, /*ssp - ref бида*/
    toUInt64( any(sid)) asid,
    any(sz) asz,
    groupArray(bref)[tn3ind] abref,
    any(geo) ageo,
    groupArray(ipint)[tn3ind] aipint_bid,
    groupArray(ipint)[tn0ind] aipint_exp,
    groupArray(ipint)[tn1ind] aipint_cl,
    any(bt) abt,
    groupArray(tn) tns, 
    indexOf(tns,3) tn3ind,
    indexOf(tns,0) tn0ind, 
    indexOf(tns,1) tn1ind,
    groupArray(sec) secs
   FROM ( /*SSP биды */ 
    SELECT uid, sec, expid, tn, sid, sz, bt, geo, ref, bref, ipint
    FROM rnd600.history_ref WHERE sec BETWEEN ${q}${begin}${q} AND ${q}${bid_end}${q} AND $where_sid_sz
    AND tn==3 AND sn==0 AND expid!=0
   UNION ALL /*SSP показы */
    SELECT uid, sec, expid, tn, sid, sz, bt, geo, ref, bref, ipint
    FROM rnd600.history_ref WHERE sec BETWEEN ${q}${begin}${q} AND ${q}${exp_end}${q} AND $where_sid_sz
    AND tn==0 AND sn==0 AND expid!=0
   UNION ALL /*SSP клики */
    SELECT uid, sec, expid, tn, sid, sz, bt, geo, ${q}${q} ref , ${q}${q} bref, ipint 
    FROM rnd600.history_ref WHERE sec BETWEEN ${q}${begin}${q} AND ${q}${click_end}${q} AND $where_sid_sz
    AND tn==1 AND sn==0 AND expid!=0
   ) 
   GROUP BY expid 
   HAVING tn3ind!=0 /*показы без бидов не нужны*/
  )
  ";
}

' "$day" "$nextday"



# --------- end of script bidy ------ 
fi

#)>>"$0.log" 2>&1
