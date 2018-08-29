# -*- coding: utf-8 -*-
# @Time    : 2018/8/10 10:02
# @Author  : 
# @简介    : 获取taxi数据源,ETL成json并转发至redis
# @File    : fetch_taxi_data.py


import json
import time
import struct
import redis
import stomp
from datetime import datetime
from math import pi, sqrt, sin, cos
# 84 to 02
a = 6378245.0
ee = 0.00669342162296594323
# World Geodetic System ==> Mars Geodetic System


def transform(wgLat, wgLon):
    """
    transform(latitude,longitude) , WGS84
    return (latitude,longitude) , GCJ02
    """
    # if outOfChina(wgLat, wgLon):
    #     mgLat = wgLat
    #     mgLon = wgLon
    #     return
    dLat = transformLat(wgLon - 105.0, wgLat - 35.0)
    dLon = transformLon(wgLon - 105.0, wgLat - 35.0)
    radLat = wgLat / 180.0 * pi
    magic = sin(radLat)
    magic = 1 - ee * magic * magic
    sqrtMagic = sqrt(magic)
    dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * pi)
    dLon = (dLon * 180.0) / (a / sqrtMagic * cos(radLat) * pi)
    mgLat = wgLat + dLat
    mgLon = wgLon + dLon
    return mgLat, mgLon


def outOfChina(lat, lon):
    if lon < 72.004 or lon > 137.8347:
        return False
    if lat < 0.8293 or lat > 55.8271:
        return False
    return True


def transformLat(x, y):
    ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * sqrt(abs(x))
    ret += (20.0 * sin(6.0 * x * pi) + 20.0 * sin(2.0 * x * pi)) * 2.0 / 3.0
    ret += (20.0 * sin(y * pi) + 40.0 * sin(y / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * sin(y / 12.0 * pi) + 320 * sin(y * pi / 30.0)) * 2.0 / 3.0
    return ret


def transformLon(x, y):
    ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * sqrt(abs(x))
    ret += (20.0 * sin(6.0 * x * pi) + 20.0 * sin(2.0 * x * pi)) * 2.0 / 3.0
    ret += (20.0 * sin(x * pi) + 40.0 * sin(x / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * sin(x / 12.0 * pi) + 300.0 * sin(x / 30.0 * pi)) * 2.0 / 3.0
    return ret
#
# conn_dest = stomp.Connection10([('192.168.11.88', 61613)])
# conn_dest.start()
# conn_dest.connect('admin', 'admin', wait=True)
# 出租车信息发送至conn_dest消息队列，与网约车等其他信息融合

conn_redis = redis.Redis(host="192.168.11.229", port=6300, db=1)


def isu2str(msg):
    total_str = "t"         # t打头，代表taxi
    for m in msg:
        total_str += "{0:^02x}".format(ord(m))
    return total_str


def bcd2time(bcd_time):
    dig = []
    for bcd in bcd_time:
        a = (ord(bcd) & 0xF0) >> 4
        b = (ord(bcd) & 0x0F) >> 0
        dig.append(a * 10 + b)
    yy, mm, dd, hh, mi, ss = dig[0:6]
    try:
        dt = datetime(2000 + yy, mm, dd, hh, mi, ss)
    except ValueError:
        print yy, mm, dd, hh, mi, ss
        return ""
    str_dt = dt.strftime("%Y-%m-%d %H:%M:%S")
    return str_dt


def trans(src):
    message = ""
    L = len(src)
    i = 0
    while i < L:
        if i < len(src) - 1:
            if ord(src[i]) == 0x7d and ord(src[i + 1]) == 0x02:
                message += chr(0x7e)
                i += 2
            elif ord(src[i]) == 0x7d and ord(src[i + 1]) == 0x01:
                message += chr(0x7d)
                i += 2
            else:
                message += src[i]
                i += 1
        else:
            message += src[i]
            i += 1
    return message


class My905Listener(object):

    def __init__(self):
        self.cnt = 0
        self.ticker = time.clock()

    def on_error(self, headers, message):
        print('received an error %s' % message)

    def on_message(self, headers, message):
        # 905协议
        message = trans(message)
        isu = message[5:11]
        body = message[13:32]
        stime = message[32:38]
        speed_time = bcd2time(stime)
        if speed_time == "":
            return
        str_isu = isu2str(isu)
        # print str_isu,
        alarm, state, lat, lng, spd, ort = struct.unpack("!IIIIHB", body)
        # print lat, lng, spd
        wglat, wglng = float(lat) / 600000, float(lng) / 600000
        if outOfChina(wglat, wglng):
            mlat, mlng = transform(wglat, wglng)
            spd = float(spd) / 10
            # print lng, lat, spd
            # 用json字符串发送
            msg_key = str(self.cnt)
            msg_dict = {'isu': str_isu, 'longi': mlng, 'lati': mlat, 'speed': spd, 'speed_time': speed_time, 'ort': ort}

            try:
                msg_json = json.dumps(msg_dict)
            except UnicodeDecodeError:
                print msg_dict
                return
            # conn_dest.send(body=msg_json, destination='/queue/fcd_position')
            conn_redis.set(name=msg_key, value=msg_json, ex=300)
            self.cnt += 1
            if self.cnt % 1000 == 0:
                self.on_cnt(time.clock() - self.ticker)
                # self.cnt = 0
                self.ticker = time.clock()

        # except Exception as e:
        #     print "exception"
        #     # print e.message

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
    conn1 = stomp.Connection10([('192.168.0.102', 61615)])
    conn1.set_listener('', FTListener())
    conn1.start()
    conn1.connect('admin', 'admin', wait=True)
    conn1.subscribe(destination='/topic/position_ft', ack='auto')

    conn = stomp.Connection10([('192.168.0.102', 61615)])
    conn.set_listener('', TYListener())
    conn.start()
    conn.connect('admin', 'admin', wait=True)
    conn.subscribe(destination='/topic/position_ty', ack='auto')

    conn2 = stomp.Connection10([('192.168.0.102', 61615)])
    conn2.set_listener('', HQListener())
    conn2.start()
    conn2.connect('admin', 'admin', wait=True)
    conn2.subscribe(destination='/topic/position_hq', ack='auto')

    while True:
        time.sleep(1)

