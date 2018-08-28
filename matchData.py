# -*- coding: utf-8 -*-
# @Time    : 2018/8/13 16:22
# @Author  : 
# @简介    : 
# @File    : matchData.py

from DBConn import oracle_util
from fcd_processor import match2road, draw_map
import estimate_speed
from datetime import datetime, timedelta
from geo import bl2xy, calc_dist, get_tti
import matplotlib.pyplot as plt
from time import clock
import numpy as np
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


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


def cmp1(data1, data2):
    if data1.stime > data2.stime:
        return 1
    elif data1.stime < data2.stime:
        return -1
    else:
        return 0


def get_all_gps_data(conn, begin_time):
    bt = clock()
    str_bt = begin_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time = begin_time + timedelta(minutes=5)
    str_et = end_time.strftime('%Y-%m-%d %H:%M:%S')
    sql = "select px, py, speed_time, state, speed, carstate, direction, vehicle_num from " \
          "TB_GPS_1805 t where speed_time >= to_date('{0}', 'yyyy-mm-dd hh24:mi:ss') " \
          "and speed_time < to_date('{1}', 'yyyy-MM-dd hh24:mi:ss') order by speed_time".format(str_bt, str_et)
    cursor = conn.cursor()
    cursor.execute(sql)
    trace = []
    for item in cursor.fetchall():
        lng, lat = map(float, item[0:2])
        if 119 < lng < 121 and 29 < lat < 31:
            px, py = bl2xy(lat, lng)
            state = int(item[3])
            stime = item[2]
            speed = float(item[4])
            car_state = int(item[5])
            ort = float(item[6])
            veh = item[7][-6:]
            taxi_data = TaxiData(veh, px, py, stime, state, speed, car_state, ort)
            trace.append(taxi_data)
    et = clock()
    print "get all gps data", et - bt
    return trace


def get_gps_data(conn, begin_time, veh):
    str_bt = begin_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time = begin_time + timedelta(minutes=5)
    str_et = end_time.strftime('%Y-%m-%d %H:%M:%S')
    sql = "select px, py, speed_time, state, speed, carstate, direction from " \
          "TB_GPS_1805 t where speed_time >= to_date('{1}', 'yyyy-mm-dd hh24:mi:ss') " \
          "and speed_time < to_date('{2}', 'yyyy-MM-dd hh24:mi:ss')" \
          " and vehicle_num = '{0}'".format(veh, str_bt, str_et)
    cursor = conn.cursor()
    cursor.execute(sql)

    trace = []
    last_point = None
    veh = veh[-6:]
    for item in cursor.fetchall():
        lng, lat = map(float, item[0:2])
        if 119 < lng < 121 and 29 < lat < 31:
            px, py = bl2xy(lat, lng)
            state = int(item[3])
            stime = item[2]
            speed = float(item[4])
            carstate = int(item[5])
            ort = float(item[6])
            taxi_data = TaxiData(veh, px, py, stime, state, speed, carstate, ort)
            trace.append(taxi_data)
    # print len(trace)
    trace.sort(cmp1)

    # new_trace = []
    # for data in trace:
    #     cur_point = data
    #     if last_point is not None:
    #         dist = calc_dist([cur_point.px, cur_point.py], [last_point.px, last_point.py])
    #         del_time = (cur_point.stime - last_point.stime).total_seconds()
    #         if data.speed > 140 or del_time < 5:            # 速度阈值 时间间隔阈值
    #             continue
    #         elif dist > 100 / 3.6 * del_time:    # 距离阈值
    #             continue
    #         elif data.speed == last_point.speed and data.speed > 0 and data.direction == last_point.direction:
    #             # 非精确
    #             continue
    #         else:
    #             data.dist = dist
    #             # del_list.append(del_time)
    #             new_trace.append(data)
    #     else:
    #         data.dist = 0
    #         new_trace.append(data)
    #     last_point = cur_point
    # # gps_point, gps_mean, gps_med = exclude_abnormal(del_list)

    return trace


def draw_trace(trace):
    x, y = [], []
    for i, data in enumerate(trace):
        plt.text(data.px + 5, data.py + 5, "{0}".format(i))
        x.append(data.px)
        y.append(data.py)
    # minx, maxx, miny, maxy = min(x), max(x), min(y), max(y)
    # plt.xlim(minx, maxx)
    # plt.ylim(miny, maxy)
    plt.plot(x, y, 'k+')


def draw_points(data_list):
    x, y = zip(*data_list)
    plt.plot(x, y, 'b+')
    for i in range(len(data_list)):
        x, y = data_list[i][0:2]


def save_speed(conn, road_speed):
    sql = "update tb_road_state set def_speed = :1 where rid = :2"
    tup_list = []
    for rid, speed in road_speed.iteritems():
        tup = (speed, rid)
        tup_list.append(tup)
    cursor = conn.cursor()
    cursor.executemany(sql, tup_list)
    conn.commit()


def get_def_speed(conn):
    sql = "select rid, speed from tb_road_def_speed"
    cursor = conn.cursor()
    cursor.execute(sql)
    def_speed = {}
    for item in cursor:
        rid = int(item[0])
        spd = float(item[1])
        def_speed[rid] = spd
    return def_speed


def main():
    conn = oracle_util.get_connection()
    veh = '浙ATB073'
    begin_time = datetime.strptime('2018-05-10 15:22:00', '%Y-%m-%d %H:%M:%S')
    trace = get_gps_data(conn, begin_time, veh)
    trace = get_all_gps_data(conn, begin_time)
    # print len(trace)

    mod_list = []
    bt = clock()
    road_temp = {}
    n0, n1, n2 = 0, 0, 0
    for i, data in enumerate(trace):
        mod_point, cur_edge, speed_list, ret = match2road(data.veh, data, i)
        for edge, spd in speed_list:
            try:
                road_temp[edge.way_id].append([spd, edge.edge_length])
            except KeyError:
                road_temp[edge.way_id] = [[spd, edge.edge_length]]
        if ret == 0:
            n0 += 1
        elif ret == 1:
            n1 += 1
        elif ret == -1:
            n2 += 1
        # speed_pool.extend(speed_list)
        if mod_point is not None:
            mod_list.append(mod_point)

    def_speed = get_def_speed(conn)
    road_speed = {}

    for rid, sp_list in road_temp.iteritems():
        W, S = 0, 0
        for sp, w in sp_list:
            S, W = S + sp * w, W + w
        print rid, S / W, len(sp_list)
        spd = S / W
        tti = def_speed[rid] / spd
        idx = get_tti(tti)
        road_speed[rid] = [S / W, idx]

    # save_speed(conn, road_speed)
    print estimate_speed.normal_cnt, estimate_speed.ab_cnt, estimate_speed.error_cnt

    et = clock()
    print "main process {0}".format(len(trace)), et - bt
    # draw_trace(trace)
    # draw_points(mod_list)
    draw_map(road_speed)
    plt.show()
    conn.close()


# main()
