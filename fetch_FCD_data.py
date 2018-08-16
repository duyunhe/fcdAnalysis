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
from map_matching import TaxiData

mm = MapMatching()


class FCDListener(object):
    def __init__(self):
        self.cnt = 0
        self.ticker = time.clock()

    def on_message(self, headers, message):
        global mm
        data = json.loads(message)
        lati, longi = float(data['lati']), float(data['longi'])
        x, y = bl2xy(lati, longi)
        speed = float(data['speed'])
        isu = data['isu'].encode('utf-8')
        data = TaxiData(x, y, 0, 0, speed, 0, 0)
        self.cnt += 1
        if self.cnt == 1000:
            self.on_cnt(time.clock() - self.ticker)
            self.cnt = 0
            self.ticker = time.clock()
        mm.PNT_MATCH(isu, data)

    def on_cnt(self, x):
        print "receive 1000 cost ", x


def main():
    conn = stomp.Connection10([('192.168.11.88', 61613)])
    conn.set_listener('', FCDListener())
    conn.start()
    conn.connect('admin', 'admin', wait=True)
    conn.subscribe(destination='/queue/fcd_position', ack='auto')

    while True:
        time.sleep(1)


main()
