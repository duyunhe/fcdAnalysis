# -*- coding: utf-8 -*-
# @Time    : 2018/8/13 16:22
# @Author  : 
# @简介    : 计算实时数据
# @File    : matchData.py

from DBConn import oracle_util
import cx_Oracle
from fcd_processor import match2road, draw_map
import estimate_speed
from datetime import datetime, timedelta
from geo import bl2xy, calc_dist
from tti import get_tti_v0, get_tti_v2
import matplotlib.pyplot as plt
from time import clock
import numpy as np
import json
import os
import redis
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
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


def get_history_speed(conn, speed_time):
    sql = "select * from TB_ROAD_HIS_SPEED where data_hour = {0} and data_weekday = {1}".format(
        speed_time.hour, speed_time.weekday()
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    speed = 0.0
    for item in cursor:
        speed = float(item[1])
    return speed


def get_all_gps_data():
    """
    从数据库中获取一段时间的GPS数据
    :return: 
    """
    end_time = datetime(2018, 5, 8, 6, 0, 0)
    conn = cx_Oracle.connect('hz/hz@192.168.11.88:1521/orcl')
    bt = clock()
    begin_time = end_time + timedelta(hours=-3)
    sql = "select px, py, speed_time, state, speed, carstate, direction, vehicle_num from " \
          "TB_GPS_1805 t where speed_time >= :1 " \
          "and speed_time < :2 "
    tup = (begin_time, end_time)
    cursor = conn.cursor()
    cursor.execute(sql, tup)
    veh_trace = {}
    static_num = {}
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
            # if veh != 'AT0956':
            #     continue
            taxi_data = TaxiData(veh, px, py, stime, state, speed, car_state, ort)
            try:
                veh_trace[veh].append(taxi_data)
            except KeyError:
                veh_trace[veh] = [taxi_data]
            try:
                static_num[veh] += 1
            except KeyError:
                static_num[veh] = 1
    et = clock()
    new_dict = {}
    for veh, trace in veh_trace.iteritems():
        trace.sort(cmp1)
        new_trace = []
        last_data = None
        for data in trace:
            esti = True
            if last_data is not None:
                dist = calc_dist([data.px, data.py], [last_data.px, last_data.py])
                dt = (data.stime - last_data.stime).total_seconds()
                if data.state == 0:
                    esti = False
                # 过滤异常
                if dt <= 10 or dt > 120:
                    esti = False
                elif dist > 100 / 3.6 * dt:  # 距离阈值
                    esti = False
                elif data.car_state == 1:  # 非精确
                    esti = False
                elif data.speed == last_data.speed and data.direction == last_data.direction:
                    esti = False
                elif dist < 20:             # GPS的误差在10米，不准确
                    esti = False
            last_data = data
            if esti:
                new_trace.append(data)
        new_dict[veh] = new_trace
    print "get all gps data", et - bt
    # print "all car:{0}, ave:{1}".format(len(static_num), len(trace) / len(static_num))
    cursor.close()
    conn.close()
    return new_dict


def get_gps_data_from_redis():
    bt = clock()
    conn = redis.Redis(host="192.168.11.229", port=6300, db=1)
    keys = conn.keys()
    new_trace = {}
    if len(keys) != 0:
        m_res = conn.mget(keys)
        et = clock()
        veh_trace = {}
        static_num = {}
        for data in m_res:
            try:
                js_data = json.loads(data)
                lng, lat = js_data['longi'], js_data['lati']
                veh, str_time = js_data['isu'], js_data['speed_time']
                speed = js_data['speed']
                stime = datetime.strptime(str_time, "%Y-%m-%d %H:%M:%S")
                state = js_data['load']
                car_state = js_data['pos']
                ort = js_data['ort']
                if 119 < lng < 121 and 29 < lat < 31:
                    px, py = bl2xy(lat, lng)
                    taxi_data = TaxiData(veh, px, py, stime, state, speed, car_state, ort)
                    try:
                        veh_trace[veh].append(taxi_data)
                    except KeyError:
                        veh_trace[veh] = [taxi_data]
                    try:
                        static_num[veh] += 1
                    except KeyError:
                        static_num[veh] = 1
            except TypeError:
                pass
        print "redis cost ", et - bt

        for veh, trace in veh_trace.iteritems():
            trace.sort(cmp1)
            filter_trace = []
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
                    filter_trace.append(data)

            new_trace[veh] = filter_trace
    # print "get all gps data {0}".format(len(veh_trace))
    # print "all car:{0}, ave:{1}".format(len(static_num), len(trace) / len(static_num))
    return new_trace


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
    if len(data_list) == 0:
        return
    x, y = zip(*data_list)
    plt.plot(x, y, 'b+')
    for i in range(len(data_list)):
        x, y = data_list[i][0:2]


def save_road_speed(conn, road_speed):
    sql = "delete from tb_road_speed"
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    sql = "insert into tb_road_speed values(:1, :2, :3, :4, :5)"
    tup_list = []
    for rid, speed_list in road_speed.iteritems():
        speed, num, tti = speed_list[:]
        if speed == float('nan') or speed == float('inf'):
            continue
        dt = datetime.now()
        tup = (rid, float('%.2f' % speed), num, tti, dt)
        tup_list.append(tup)
    cursor.executemany(sql, tup_list)
    conn.commit()
    print "road speed updated!"


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


def save_roadspeed_bak(conn, speed_dict):
    cursor = conn.cursor()
    sql = "insert into tb_road_speed_bak values(:1, :2, :3, :4, :5)"
    tup_list = []
    for rid, speed_list in speed_dict.iteritems():
        speed, num, tti = speed_list[:]
        if speed == float('nan') or speed == float('inf'):
            continue
        DT = datetime.now()
        tup = (rid, speed, num, tti, DT)
        tup_list.append(tup)
    cursor.executemany(sql, tup_list)
    conn.commit()


def save_road_speed_pre(conn, road_speed, stime):
    sql = "insert into tb_road_speed_pre values(:1, :2, :3)"
    tup_list = []
    for rid, speed in road_speed.iteritems():
        if speed[0] == float('nan') or speed[0] == float('inf'):
            continue
        sp = float('%.2f' % speed[0])
        tup = (rid, sp, stime)
        # print tup
        tup_list.append(tup)
    print len(tup_list)
    cursor = conn.cursor()
    cursor.executemany(sql, tup_list)
    conn.commit()


def get_road_speed(conn, road_speed_detail):
    def_speed = get_def_speed(conn)
    road_speed = {}
    his_speed = get_history_speed(conn, datetime.now())
    for rid, sp_list in road_speed_detail.iteritems():
        W, S = 0, 0
        for sp, w in sp_list:
            S, W = S + sp * w, W + w
        spd = S / W
        n_sample = len(sp_list)
        if n_sample < 10:
            spd = (spd * n_sample + his_speed * 20) / (n_sample + 20)
        # radio = def_speed[rid] / spd
        idx = get_tti_v2(spd, def_speed[rid])
        # print rid, S / W, len(sp_list), radio, idx
        road_speed[rid] = [spd, n_sample, idx]
    return road_speed


def save_global_tti(conn, road_speed, db_time):
    """
    通过road_speed 得到全城市的tti指数
    :param conn: 
    :param road_speed: { rid: [speed, n_sample, tti] }
    rid: int road id
    speed: float 道路速度
    n_sample: int 道路上的采样点数量
    tti: 道路的出行指数
    :param db_time 写库时间
    :return: 
    """
    road_dist = {}
    cursor = conn.cursor()
    sql = "select rid, road_desc from tb_road_state"
    cursor.execute(sql)
    for item in cursor:
        rid = int(item[0])
        dist = float(item[1])
        road_dist[rid] = dist
    sql = "insert into TB_TRAFFIC_INDEX values(:1, :2, :3, :4, :5)"
    # tti, fast_speed, main_speed, congest_ratio
    S, T, W = .0, .0, .0        # speed, tti, dist
    congest_dist = .0
    for rid, item in road_speed.iteritems():
        speed, n, tti = item[:]
        w = road_dist[rid]
        W += w
        if tti > 4.0:
            congest_dist += w
        S, T = S + speed * w, T + tti * w
    mean_speed, mean_tti = S / W, T / W
    congest_ratio = congest_dist * 100.0 / W
    tup = (mean_tti, 50, mean_speed, congest_ratio, db_time)
    cursor.execute(sql, tup)
    conn.commit()
    cursor.close()


def main():
    run_time = datetime.now()
    # trace_dict = get_all_gps_data()
    conn = oracle_util.get_connection()
    # trace = get_gps_data(conn, begin_time, veh)
    trace_dict = get_gps_data_from_redis()

    # print len(trace)
    mod_list = []
    bt = clock()
    road_temp = {}
    # sql = "insert into tb_road_speed_detail values(:1, :2, :3, :4)"
    tup_list = []
    for veh, trace in trace_dict.iteritems():
        for i, data in enumerate(trace):
            mod_point, cur_edge, speed_list, ret = match2road(data.veh, data, i)
            for edge, spd in speed_list:
                try:
                    road_temp[edge.way_id].append([spd, edge.edge_length])
                except KeyError:
                    road_temp[edge.way_id] = [[spd, edge.edge_length]]
                tup = (edge.way_id, spd, veh, data.stime)
                tup_list.append(tup)
            if mod_point is not None:
                mod_list.append(mod_point)

    print "update speed detail"
    # 每条道路的速度
    road_speed = get_road_speed(conn, road_temp)
    print "matchData1.py main", estimate_speed.normal_cnt, estimate_speed.ab_cnt, estimate_speed.error_cnt
    # 当前路况
    save_road_speed(conn, road_speed)
    # 当前交通指数
    save_global_tti(conn, road_speed, run_time)

    et = clock()
    print "main process {0}".format(len(trace_dict)), et - bt
    # draw_trace(dtrace)
    # draw_points(mod_list)
    # draw_map(road_speed)
    # plt.show()
    conn.close()


if __name__ == '__main__':
    logging.basicConfig()
    scheduler = BlockingScheduler()
    # scheduler.add_job(tick, 'interval', days=1)
    scheduler.add_job(main, 'cron', minute='*/5')
    main()
    try:
        scheduler.start()
    except SystemExit:
        pass
