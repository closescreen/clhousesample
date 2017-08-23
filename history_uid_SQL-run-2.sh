set +x
set -o pipefail
set -u

# Набор SQL, разделенных пустой строкой созраняет в файлы sqldir="../../reg_history_uid/$day/<N>" по одному SQL на файл.
# можно сохранять SQL без указания сервера (одна версия SQL-инструкций на все сервера) 

day=${1:?DAY!} #"2017-03-20"
sqldir="../../reg_history_uid/$day"
mkdir -p $sqldir

perl -MDate::Calc -Mstrict -e'
use Date::Calc qw/Today_and_Now Delta_DHMS/;
my $sql;
my $sqldir = shift() or die "sqldir!";
my $cnt = 0;
while(<STDIN>){
 if ( /^\s*$/ and $sql ){
    $cnt++;
    end_of_sql($sql, $sqldir, sprintf("sql_%04d",$cnt));
    $sql = "";
 }else{
    $sql .= $_;
 }
}

sub end_of_sql{
 my $sql = shift();
 my $sqldir = shift();
 my $name = shift();
 my $f = "$sqldir/$name.sql";
 return 0 if -s $f; # не перезаписывать файлы
 open( SQL, ">", $f) or die "Cant open $f: $!";
 print SQL $sql;
 close SQL;
}

' "$sqldir"

# perl -lane'BEGIN{$/="\n\n"}; warn $_; open(H,"| clickhouse-client") or die $!; print H $_; print "== ok $. ==\n\n"'
 