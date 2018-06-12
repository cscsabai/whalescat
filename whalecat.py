#!/usr/bin/python3
# -*- coding: utf-8 -*-
from __future__ import print_function

#Categorize whales!

import datetime
import time
import json
import sys
import socket, os
import http.client
import urllib.request, urllib.parse, urllib.error
import pymysql as mdb

from datetime import date, timedelta, timezone

lib_path = os.path.abspath('/secure/conf')
import ccoin_conf as cconf

mdb.install_as_MySQLdb();
debug_mode = 0;

def get_trans(tx, addr_id,  addr):
    _balance = tx['final_balance'] / 100000000;
    cur.execute ('UPDATE whales set balance=%.08f where id=%d' % (_balance, addr_id));
    for j in tx['txs']:
      _ret = 0;
      for i in j['inputs']:
        if ('prev_out' in i and 'addr' in i['prev_out'] and i['prev_out']['addr'] == addr): _ret = _ret - int(i['prev_out']['value'])/100000000 ;
      for i in j['out']:
        if ('addr' in i and i['addr'] == addr): _ret = _ret + int(i['value'])/100000000 ;
      _balance = _balance + _ret;
      if ('block_height' in j): cur.execute ("INSERT INTO wtrans VALUES(0, %d, %d, FROM_UNIXTIME(%d), %.08f, %.08f)" % (addr_id, j['block_height'],j['time'],_ret,_balance));
    con.commit();

def fmt_valuechange(a , b):
    if (b == 0):
      return ("   VOID");
    if (a == 0):
      return ("  RESET");
    _r = ((a/b)-1)*100;
    return ( "%7.2f" % (_r));

def fmt_humanbtc(a):
    b = float(int(a*100)/100);
    _ret = "";
    if ((b>1000) or (b<(-1000))):
      _ret = "%.02fK" % (b/1000);
    else:
      _ret = "%.02f" % (b);
    return (_ret); 

try:
   _d = date.today();
   _now = datetime.datetime.now().time()
   _day = datetime.datetime(_d.year, _d.month, _d.day, _now.hour, _now.minute, _now.second);
   _tsday = (_day.strftime("%s"));
   _ts_1h = ((_day - timedelta(minutes=60)).strftime("%s"));
   _ts_3h = ((_day - timedelta(hours=3)).strftime("%s"));
   _ts_12h = ((_day - timedelta(hours=12)).strftime("%s"));
   _ts_1d = ((_day - timedelta(days=1)).strftime("%s"));
   _ts_3d = ((_day - timedelta(days=3)).strftime("%s"));
   _ts_1w = ((_day - timedelta(days=7)).strftime("%s"));
   _ts_1m = ((_day - timedelta(days=30)).strftime("%s"));
   _ts_3m = ((_day - timedelta(days=90)).strftime("%s"));

               
   cconf.__sec_initialize(token="WHALECAT-demo")
   con = mdb.connect(cconf.host, cconf.user, cconf.pass, cconf.db)
   con.autocommit(False);
   cur = con.cursor()
   cur.execute("SELECT VERSION()")
   ver = cur.fetchone()

   cur.execute('SELECT id,addr from whales where valid=1 order by balance desc')
   a = cur.fetchall();
   _1m_b_chg = 0;
   
   _whale_stat_volumen_change = [0,0,0,0,0,0,0,0,0,0];
   _whale_stat_change = [0,0,0,0,0,0,0,0,0,0];
   _num_of_wiped_whales = 0;
   _max_volumen_of_wiped_whales = 0;
   _num_real_whales = 0;
   _num_addrs = 0;
   _currbalance_all = 0;
   _maxbalance_all = 0;
   _invalidated = 0;
   print ("Legends:\n - P: time periods since still exists and alive the address # 0: 0-1h, 1: 1h-3h, 2: 3h-12h, 3: 12h-24h, 4: 1d-3d, 5: 3d-1w, 6: 1w-1m,  7: 1-3m, 8:>3m\n - CurV: Current Balance of the address (now)\n - MaxV: Max blance in the last 3 months"); 
   print (" - CHG%: diff between current and max(3m) balance");
   print (" - 1h%: balance change in the last hour");
   print (" - 3h%: balance change in last three hours");
   print (" [...] ");
   print (" - 3m%: balance change in last three months");
   print ("--------------------------------------------------------------------------------------------------------------------");
   print ("%34s" % "address", "%s % 7s % 7s %s %s %s %s %s %s %s %s " % ("P", "CurV", "MaxV", "   CHG% ", "   1h% ", "   3h% ","  12h% ","   1d% ","   1w% ", "   1m% ", "   3m% "))
   print ("--------------------------------------------------------------------------------------------------------------------");
   for i in a:
       cur.execute('SELECT tid, wid, blockid, UNIX_TIMESTAMP(date), amount, balance from wtrans where wid=%d order by date desc'%(i[0]));
       b = cur.fetchall();
       _curr_balance = b[0][5];
       _max_balance = _curr_balance;
       _init_balance = _curr_balance
       _p_balance = [_curr_balance, 0,0,0,0,0,0,0,0,0];
       _num_addrs = _num_addrs +1;
       _currbalance_all = _currbalance_all + _curr_balance;
       _period = 0;    # 0: 0-1h, 1: 1h-3h, 2: 3h-12h, 3: 12h-24h, 4: 1d-3d, 5: 3d-1w, 6: 1w-1m,  7: 1-3m, 8:>3m
       for j in b:
         _curr_balance = _curr_balance - float(j[4])
         if (_curr_balance > _max_balance): _max_balance = _curr_balance;
         if (_period == 0 and j[3] < int(_ts_1h)): 
            _p_balance[1] = _curr_balance + float(j[4])
            _period = 1;
         if (_period == 1 and j[3] < int(_ts_3h)):
            _p_balance[2] = _curr_balance + float(j[4])            
            _period = 2;
         if (_period == 2 and j[3] < int(_ts_12h)):
            _p_balance[3] = _curr_balance + float(j[4])
            _period = 3;
         if (_period == 3 and j[3] < int(_ts_1d)):
            _p_balance[4] = _curr_balance + float(j[4])
            _period = 4;
         if (_period == 4 and j[3] < int(_ts_3d)):
            _p_balance[5] = _curr_balance + float(j[4])
            _period = 5;
         if (_period == 5 and j[3] < int(_ts_1w)):
            _p_balance[6] = _curr_balance + float(j[4])
            _period = 6;
         if (_period == 6 and j[3] < int(_ts_1m)):
            _p_balance[7] = _curr_balance + float(j[4])
            _period = 7;
         if (_period == 7 and j[3] < int(_ts_3m)):
            _p_balance[8] = _curr_balance + float(j[4])
            _period = 8;
       _maxbalance_all = _maxbalance_all + _max_balance;
       if (_period < 7 and _curr_balance < 1):
          cur.execute('UPDATE whales set valid=0 where id="%d"'%(i[0]));
          _invalidated = _invalidated+1;          
          con.commit();
       if (_period > 7):      # addr still exists since 3M
           _whale_stat_volumen_change[0] = _whale_stat_volumen_change[0] + _p_balance[0];
           _whale_stat_volumen_change[1] = _whale_stat_volumen_change[1] + _p_balance[1];
           _whale_stat_volumen_change[2] = _whale_stat_volumen_change[2] + _p_balance[2];
           _whale_stat_volumen_change[3] = _whale_stat_volumen_change[3] + _p_balance[3];
           _whale_stat_volumen_change[4] = _whale_stat_volumen_change[4] + _p_balance[4];
           _whale_stat_volumen_change[5] = _whale_stat_volumen_change[5] + _p_balance[5];
           _whale_stat_volumen_change[6] = _whale_stat_volumen_change[6] + _p_balance[6];
           _whale_stat_volumen_change[7] = _whale_stat_volumen_change[7] + _p_balance[7];
           _num_real_whales = _num_real_whales + 1;
           if ( _p_balance[0] < 1):   # Wiped out!!!
              _num_of_wiped_whales = _num_of_wiped_whales +1
              _max_volumen_of_wiped_whales = _max_volumen_of_wiped_whales + _max_balance;
       if _curr_balance < 0:
          _p_balance[8] = 0;
       else: _p_balance[8] = _curr_balance 
       print ("%34s" % i[1], "%d % 7s % 7s %s %s %s %s %s %s %s %s " % (_period, fmt_humanbtc(_init_balance), fmt_humanbtc(_max_balance), \
              fmt_valuechange(_init_balance, _max_balance), fmt_valuechange(_p_balance[0],_p_balance[1]), fmt_valuechange(_p_balance[1],_p_balance[2]), \
              fmt_valuechange(_p_balance[2],_p_balance[3]), fmt_valuechange(_p_balance[3],_p_balance[4]), fmt_valuechange(_p_balance[4],_p_balance[5]), \
              fmt_valuechange(_p_balance[5],_p_balance[6]), fmt_valuechange(_p_balance[6],_p_balance[7])));

   print ("--------------------------------------------------------------------------------------------------------------------");
   print ("Reviewed (potential whales) addresses: count: %d  current balance: % 7s BTC,  max balance (in last 3months): % 7s BTC " % (_num_addrs, fmt_humanbtc(_currbalance_all), fmt_humanbtc(_maxbalance_all)));
   print ("--------------------------------------------------------------------------------------------------------------------");
   print ("Num of real whales (exists more than 3 months):", _num_real_whales);
   print (" - Percent change : now-1h:%s%% (change: % 7s BTC, total: % 7s BTC) " % ( fmt_valuechange(_whale_stat_volumen_change[0],_whale_stat_volumen_change[1]), fmt_humanbtc(_whale_stat_volumen_change[0]-_whale_stat_volumen_change[1]),fmt_humanbtc(_whale_stat_volumen_change[0])));
   print (" - Percent change :  1h-3h:%s%% (change: % 7s BTC, total: % 7s BTC) " % ( fmt_valuechange(_whale_stat_volumen_change[1],_whale_stat_volumen_change[2]), fmt_humanbtc(_whale_stat_volumen_change[1]-_whale_stat_volumen_change[2]),fmt_humanbtc(_whale_stat_volumen_change[1])));
   print (" - Percent change : 3h-12h:%s%% (change: % 7s BTC, total: % 7s BTC) " % ( fmt_valuechange(_whale_stat_volumen_change[2],_whale_stat_volumen_change[3]), fmt_humanbtc(_whale_stat_volumen_change[2]-_whale_stat_volumen_change[3]),fmt_humanbtc(_whale_stat_volumen_change[2])));
   print (" - Percent change : 12h-1d:%s%% (change: % 7s BTC, total: % 7s BTC) " % ( fmt_valuechange(_whale_stat_volumen_change[3],_whale_stat_volumen_change[4]), fmt_humanbtc(_whale_stat_volumen_change[3]-_whale_stat_volumen_change[4]),fmt_humanbtc(_whale_stat_volumen_change[3])));
   print (" - Percent change :  1d-1w:%s%% (change: % 7s BTC, total: % 7s BTC) " % ( fmt_valuechange(_whale_stat_volumen_change[4],_whale_stat_volumen_change[5]), fmt_humanbtc(_whale_stat_volumen_change[4]-_whale_stat_volumen_change[5]),fmt_humanbtc(_whale_stat_volumen_change[4])));
   print (" - Percent change :  1w-1m:%s%% (change: % 7s BTC, total: % 7s BTC) " % ( fmt_valuechange(_whale_stat_volumen_change[5],_whale_stat_volumen_change[6]), fmt_humanbtc(_whale_stat_volumen_change[5]-_whale_stat_volumen_change[6]),fmt_humanbtc(_whale_stat_volumen_change[5])));
   print (" - Percent change :  1m-3m:%s%% (change: % 7s BTC, total: % 7s BTC) " % ( fmt_valuechange(_whale_stat_volumen_change[6],_whale_stat_volumen_change[7]), fmt_humanbtc(_whale_stat_volumen_change[6]-_whale_stat_volumen_change[7]),fmt_humanbtc(_whale_stat_volumen_change[6])));
   print (" - Num of Wiped out whales addresses:", _num_of_wiped_whales, " Max amount of bitcoins on these addresses: ", _max_volumen_of_wiped_whales);
   print (" - Invalidated addrs (possible tumbling or btc mixer services): ", _invalidated);
finally:
   if 'cur' in globals(): cur.close();
   if 'con' in globals(): con.close();
      

