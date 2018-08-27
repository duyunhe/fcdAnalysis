# -*- coding: utf-8 -*-
# @Time    : 2018/8/10 16:00
# @Author  : 
# @简介    : 获取浮动车定位信息
# @File    : fetch_FCD_data.py


import json
import time
import stomp
from geo import bl2xy
from map_matching import MapMatching
from matchData import TaxiData
from fcd_processor import match2road
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler


road_temp = {}


def bcd2time(bcd_time):
    dig = []
    for bcd in bcd_time:
        a = (ord(bcd) & 0xF0) >> 4
        b = (ord(bcd) & 0x0F) >> 0
        dig.append(a * 10 + b)
    yy, mm, dd, hh, mi, ss = dig[0:6]
    dt = datetime(year=yy, month=mm, day=dd, hour=hh, minute=mi, second=ss)
    return dt


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

        self.cnt += 1
        if self.cnt == 1000:
            self.on_cnt(time.clock() - self.ticker)
            self.cnt = 0
            self.ticker = time.clock()

    def on_cnt(self, x):
        print "receive 1000 cost ", x


def main():
    conn = stomp.Connection10([('192.168.11.88', 61613)])
    conn.set_listener('', FCDListener())
    conn.start()
    conn.connect('admin', 'admin', wait=True)
    conn.subscribe(destination='/queue/fcd_position', ack='auto')
    sch = BackgroundScheduler()
    sch.add_job(job, 'interval', minutes=2)

    while True:
        time.sleep(1)


main()
