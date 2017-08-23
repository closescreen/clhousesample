package TimeEnd;
use Date::Calc qw/Add_Delta_DHMS/;

sub hours($$$){
 # прибавляет к starthour nhours минус 1 секунду
 my $start = shift or die "start hour!"; # 2017-03-02T00:00:00
 my $nhours = shift; # 4
 my $nminits = shift||0; # 5
 my ($y0,$m0,$d0,$h0,$mi0,$s0) = $start=~/(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)/;
 my ($y1,$m1,$d1,$h1,$mi1,$s1) = Add_Delta_DHMS($y0,$m0,$d0,$h0,$mi0,$s0,  0,$nhours,$nminits,-1);
 return sprintf("%d-%02d-%02dT%02d:%02d:%02d", $y1,$m1,$d1,$h1,$mi1,$s1);
}


#($year,$month,$day, $hour,$min,$sec) =
#      Add_Delta_DHMS($year,$month,$day, $hour,$min,$sec,
#                     $Dd,$Dh,$Dm,$Ds);
 


1;