# -*- coding: utf-8 -*-
# @Time    : 2018/8/10 10:02
# @Author  : 
# @简介    : 获取taxi数据源,ETL成json并转发至目的地队列
# @File    : fetch_taxi_data.py


import json
import time
import struct
import stomp

conn_dest = stomp.Connection10([('192.168.11.88', 61613)])
conn_dest.start()
conn_dest.connect('admin', 'admin', wait=True)
# 出租车信息发送至conn_dest消息队列，与网约车等其他信息融合


def isu2str(msg):
    total_str = "t"         # t打头，代表taxi
    for m in msg:
        total_str += "{0:^02x}".format(ord(m))
    return total_str


class My905Listener(object):

    def __init__(self):
        self.cnt = 0
        self.ticker = time.clock()

    def on_error(self, headers, message):
        print('received an error %s' % message)

    def on_message(self, headers, message):
        # 905协议
        try:
            isu = message[5:11]
            body = message[13:31]
            str_isu = isu2str(isu)
            # print str_isu,
            alarm, state, lat, lng, spd = struct.unpack("!IIIIH", body)
            # print lat, lng, spd
            lat, lng = float(lat) / 600000, float(lng) / 600000
            spd = float(spd) / 10
            # print lng, lat, spd
            # 用json字符串发送
            msg_dict = {}
            msg_dict['isu'] = str_isu
            msg_dict['longi'] = lng
            msg_dict['lati'] = lat
            msg_dict['speed'] = spd
            msg_json = json.dumps(msg_dict)
            conn_dest.send(body=msg_json, destination='/queue/fcd_position')

            self.cnt += 1
            if self.cnt == 1000:
                self.on_cnt(time.clock() - self.ticker)
                self.cnt = 0
                self.ticker = time.clock()

        except Exception as e:
            print e.message

    def on_cnt(self, x):
        pass


class FTListener(My905Listener):
    def on_cnt(self, x):
        print "ft 1000 cost ", x


class TYListener(My905Listener):
    def on_cnt(self, x):
        print "ty 1000 cost ", x


class HQListener(My905Listener):
    def on_cnt(self, x):
        print "hq 1000 cost ", x


if __name__ == '__main__':
    conn1 = stomp.Connection10([('192.168.0.102', 61613)])
    conn1.set_listener('', FTListener())
    conn1.start()
    conn1.connect('admin', 'admin', wait=True)
    conn1.subscribe(destination='/topic/position_ft', ack='auto')

    conn = stomp.Connection10([('192.168.0.102', 61613)])
    conn.set_listener('', TYListener())
    conn.start()
    conn.connect('admin', 'admin', wait=True)
    conn.subscribe(destination='/topic/position_ty', ack='auto')

    conn2 = stomp.Connection10([('192.168.0.102', 61613)])
    conn2.set_listener('', HQListener())
    conn2.start()
    conn2.connect('admin', 'admin', wait=True)
    conn2.subscribe(destination='/topic/position_hq', ack='auto')

    while True:
        time.sleep(1)

