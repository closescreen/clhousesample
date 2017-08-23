#!/usr/bin/python
#coding=utf-8

import re
import sys
sys.path.append('/usr/local/rle/lib/python')

pattern = '%Y-%m-%dT%H'

import time
if len(sys.argv) < 4:
    print "Usage: ", sys.argv[0], " starttime usergroupfrom usergroupto \n\n\tTime format is ", pattern
    # like: history_ref_from_history_v04.py 2017-06-25T05 129 256 [deb]
    # usergroupfrom/usergroupto - юзергруппы от и до включительно
    sys.exit(1)

deb = False
if len(sys.argv)>=5:
    deb = True # наличие любого argv[4] параметра включает deb (очень тупо)

r = re.compile(r".*?(\d\d\d\d)\D+(\d\d)\D+(\d\d)\D+(\d\d).*?")

m1 = r.match(sys.argv[1])
if m1 is None: 
    sys.stderr.write("bad hour " + sys.argv[1])
    sys.exit(1)

if m1: (y1,m1,d1,h1) = m1.groups()

import datetime
d1 = datetime.datetime(int(y1), int(m1), int(d1), int(h1),0,0)
d2 = d1 + datetime.timedelta( hours=1 )
begin = time.mktime( d1.timetuple())
end = time.mktime( d2.timetuple())

ug_from = int( sys.argv[2] )
ug_to = int( sys.argv[3] )
if ug_from<1 or ug_to<1 or ug_from>ug_to:
    sys.stderr.write("bad usergroups: " + sys.argv[2] + " " + sys.argv[3] )
    sys.exit(1)

groups = range( ug_from, ug_to+1 )

# debug hours:
if deb: print >> sys.stderr,  "%s - %s" % (datetime.datetime.fromtimestamp( begin), datetime.datetime.fromtimestamp( end)) 


from adriver import history

# ... и инициализируем (можно указать путь до конфига, по умочанию стандартный)
#history.loadConfig('/usr/local/rle/etc/history.conf')
catalog = history.Catalog()
catalog.setHosts([('hist.adriver.x', 19007)])
catalog.start()
links = catalog.getLinks(begin, end)
reader = history.Reader(links)


i = 0
for e in reader.getEvents( groups=groups ):
    
    user_ = e.user
#    if user_==0: continue # теперь есть фильтр по группам
#    ugroup_ = user_ % 256 + 1
#    if ( ugroup_ < ug_from ) or ( ugroup_ > ug_to): continue

    # удалить ломающие символы
    bref_ = re.sub( r'\n|\t|\'|\"|\\', '', e.backref ) if e.backref is not None else ""
    ref_ = re.sub( r'\n|\t|\'|\"|\\', '', e.referer ) if e.referer is not None else ""
    custom_ = re.sub( r'\n|\t|\'|\"|\\', '', e.custom) if e.custom is not None else ""

    sid_ = e.site if e.site is not None else 0
    
    sz_ = e.sitezone if e.sitezone is not None else 0
    expid_ = e.expid if e.expid is not None else 0
    tn_ = e.type if e.type is not None else 0
    sn_ = e.status if e.status is not None else 0
    stn_ = e.subtype if e.subtype is not None else 0
    geo_ = e.geozone if e.geozone is not None else 0
    ip_ = e.userip if e.userip is not None else 0
    pz_ = e.pagezone if e.pagezone is not None else 0
    ad_ = e.ad if e.ad is not None else 0
    net_ = e.network if e.network is not None else 0
    exppr_ = e.exposureprice if e.exposureprice is not None else 0
    winexppr_ = e.winexposureprice if e.winexposureprice is not None else 0
    secondpr_ = e.secondprice if e.secondprice is not None else 0
    floor_ = e.effectivesspbidfloor if e.effectivesspbidfloor is not None else 0
    ddid_ = e.sspdirectdealid if e.sspdirectdealid is not None else 0
    
    bt_ = e.bannertype if e.bannertype is not None else 0

    # Yandex домены без протокола, добавить протокол:
    if sid_==197671 and not "://" in ref_: ref_ = "http://" + ref_
    
    # Google 187537 домены без протокола (бывают даже без реферера, одни кавычки - тогда пусто)
    # if sid_==187537 and ref похож на 8044804918583459066.1.google
    if sid_==187537 and ref_.endswith(".google") : ref_ = "http://" + ref_

    # привести к формату TabSeparated
    i=i+1
    print "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % \
	(e.user,e.time,expid_,tn_,sn_,stn_,ref_,sid_, sz_,bref_,geo_,ip_,pz_,bt_,ad_,net_,exppr_,winexppr_,secondpr_,floor_,ddid_,custom_)
    #      1       2      3    4   5   6    7    8    9    10    11  12  13  14  15   16    17      18        19       20    21     22

    if deb and i % 1000000 == 0: sys.stderr.write("%d " % i )	









