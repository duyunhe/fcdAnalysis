# -*- coding: utf-8 -*-
# @Time    : 2018/8/10 16:00
# @Author  : 
# @简介    : 获取浮动车定位信息
# @File    : fetch_FCD_data.py


import json
import time
import stomp
from geo import bl2xy
from tti import get_tti
from DBConn import oracle_util
from matchData import TaxiData, get_def_speed
from fcd_processor import match2road
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler


road_temp = {}
data_cnt = 0


def bcd2time(bcd_time):
    dig = []
    for bcd in bcd_time:
        a = (ord(bcd) & 0xF0) >> 4
        b = (ord(bcd) & 0x0F) >> 0
        dig.append(a * 10 + b)
    yy, mm, dd, hh, mi, ss = dig[0:6]
    dt = datetime(year=yy, month=mm, day=dd, hour=hh, minute=mi, second=ss)
    return dt


def job():
    global data_cnt
    print "update road speed"
    print "receive data {0}".format(data_cnt)
    data_cnt = 0
    sql = "delete from tb_road_speed"
    db = oracle_util.get_connection()
    def_speed_dict = get_def_speed(db)
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()

    tup_list = []
    global road_temp
    for rid, sp_list in road_temp.iteritems():
        W, S = 0, 0
        for sp, w in sp_list:
            S, W = S + sp * w, W + w
        print rid, S / W, len(sp_list)
        def_spd = def_speed_dict[rid]
        spd = S / W
        tti = get_tti(def_spd / spd)
        tup = (rid, spd, len(sp_list), tti)
        tup_list.append(tup)

    road_temp = {}
    sql = "insert into tb_road_speed values(:1, :2, :3, :4)"
    cursor.executemany(sql, tup_list)
    db.commit()
    db.close()


class FCDListener(object):
    def __init__(self):
        self.cnt = 0
        self.ticker = time.clock()

    def on_message(self, headers, message):
        data = json.loads(message)
        try:
            lati, longi = float(data['lati']), float(data['longi'])
            x, y = bl2xy(lati, longi)
            speed = float(data['speed'])
            str_time = data['speed_time']
            speed_time = datetime.strptime(str_time, "%Y-%m-%d %H:%M:%S")
            isu = data['isu'].encode('utf-8')
        except KeyError:
            return
        data = TaxiData(isu, x, y, speed_time, 0, speed, 0, 0)
        _, _, speed_list, _ = match2road(isu, data, self.cnt)
        for edge, spd in speed_list:
            try:
                road_temp[edge.way_id].append([spd, edge.edge_length])
            except KeyError:
                road_temp[edge.way_id] = [[spd, edge.edge_length]]

        global data_cnt
        data_cnt += 1
        if data_cnt % 1000 == 0:
            self.on_cnt(time.clock() - self.ticker)
            self.ticker = time.clock()

    def on_cnt(self, x):
        print "receive 1000 cost ", x


def main():
    logging.basicConfig()
    log = logging.getLogger('apscheduler.executors.default')
    sch = BackgroundScheduler()
    sch.add_job(job, 'interval', minutes=5)
    sch.start()
    conn = stomp.Connection10([('192.168.11.88', 61613)])
    conn.set_listener('', FCDListener())
    conn.start()
    conn.connect('admin', 'admin', wait=True)
    conn.subscribe(destination='/queue/fcd_position', ack='auto')

    while True:
        time.sleep(1)


main()
