# -*- coding: utf-8 -*-
# @Time    : 2018/10/29 10:54
# @Author  : yhdu@tongwoo.cn
# @简介    : 
# @File    : multi.py


import multiprocessing
from ctypes import *
from datetime import datetime, timedelta
import cx_Oracle
from time import clock
import numpy as np


dll = WinDLL("CoordTransDLL.dll")


class BLH(Structure):
    _fields_ = [("b", c_double),
                ("l", c_double),
                ("h", c_double)]


class XYZ(Structure):
    _fields_ = [("x", c_double),
                ("y", c_double),
                ("z", c_double)]


def bl2xy(b, l):
    """
    :param b: latitude
    :param l: longitude
    :param dll: 
    :return: x, y
    """
    blh = BLH()
    blh.b = float(b)
    blh.l = float(l)
    blh.h = 0
    xyz = XYZ()
    dll.WGS84_BLH_2_HZ_xyH(blh, byref(xyz))
    y, x = xyz.x, xyz.y
    return x, y


class TaxiData:
    def __init__(self, veh, px, py, stime, state, speed, car_state, direction):
        self.veh = veh
        self.px, self.py, self.stime, self.state, self.speed = px, py, stime, state, speed
        self.stop_index, self.dist, self.car_state, self.direction = 0, 0, car_state, direction
        self.angle = 0

    def set_index(self, index):
        self.stop_index = index

    def set_angle(self, angle):
        self.angle = angle


def calc_dist(pt0, pt1):
    """
    计算两点距离
    :param pt0: [x0, y0]
    :param pt1: [x1, y1]
    :return: 
    """
    v0 = np.array(pt0)
    v1 = np.array(pt1)
    dist = np.linalg.norm(v0 - v1)
    return dist


def cmp1(data1, data2):
    if data1.stime > data2.stime:
        return 1
    elif data1.stime < data2.stime:
        return -1
    else:
        return 0


def work(trace_list):
    dll = WinDLL("CoordTransDLL.dll")
    for trace in trace_list:
        new_trace = []
        for position_data in trace:
            veh, lat, lng, stime, state, speed, car_state, ort = position_data[:]
            px, py = bl2xy(lat, lng, dll)
            taxi_data = TaxiData(veh, px, py, stime, state, speed, car_state, ort)
            new_trace.append(taxi_data)

        new_trace.sort(cmp1)
        last_data = None
        for data in trace:
            esti = True
            dist = 0
            if last_data is not None:
                dist = calc_dist([data.px, data.py], [last_data.px, last_data.py])
                dt = (data.stime - last_data.stime).total_seconds()
                if data.state == 0:
                    esti = False
                # 过滤异常
                if dt <= 10 or dt > 180:
                    esti = False
                elif dist > 100 / 3.6 * dt:  # 距离阈值
                    esti = False
                elif data.car_state == 1:  # 非精确
                    esti = False
                elif data.speed == last_data.speed and data.direction == last_data.direction:
                    esti = False
                elif dist < 20:  # GPS的误差在10米，不准确
                    esti = False
            last_data = data
            if esti:
                new_trace.append(data)
                # print i, dist
                # i += 1
    print "over"


def get_all_gps_data(begin_time):
    """
    从数据库中获取一段时间的GPS数据
    :return: 
    """
    # begin_time = datetime(2018, 5, 1, 0, 0, 0)
    conn = cx_Oracle.connect('hz/hz@192.168.11.88:1521/orcl')
    bt = clock()
    end_time = begin_time + timedelta(days=1)
    # sql = "select px, py, speed_time, state, speed, carstate, direction, vehicle_num from " \
    #       "TB_GPS_1805 t where speed_time >= :1 " \
    #       "and speed_time < :2 and vehicle_num = '浙AT7902' order by speed_time "

    sql = "select px, py, speed_time, state, speed, carstate, direction, vehicle_num from " \
          "TB_GPS_1805 t where speed_time >= :1 and speed_time < :2"

    tup = (begin_time, end_time)
    cursor = conn.cursor()
    cursor.execute(sql, tup)
    veh_trace = {}

    for item in cursor.fetchall():
        lng, lat = map(float, item[0:2])
        if 119 < lng < 121 and 29 < lat < 31:
            px, py = bl2xy(lat, lng)
            px, py = round(px, 2), round(py, 2)
            state = int(item[3])
            stime = item[2]
            speed = float(item[4])
            car_state = int(item[5])
            ort = float(item[6])
            veh = item[7][-6:]
            taxi_data = [veh, px, py, stime, state, speed, car_state, ort]
            try:
                veh_trace[veh].append(taxi_data)
            except KeyError:
                veh_trace[veh] = [taxi_data]

    et = clock()
    str_dt = begin_time.strftime("%Y-%m-%d")
    fp = open("./data/{0}.txt".format(str_dt), "w")
    for veh, trace in veh_trace.iteritems():
        for taxi_data in trace:
            str_data = ','.join([str(item) for item in taxi_data])
            fp.write(str_data)
            fp.write("\n")
    fp.close()
    print "get all gps data", et - bt
    cursor.close()
    conn.close()


def gene():
    begin_time = datetime(2018, 5, 1, 0, 0, 0)
    for i in range(31):
        get_all_gps_data(begin_time)
        begin_time += timedelta(days=1)


if __name__ == '__main__':
    gene()
