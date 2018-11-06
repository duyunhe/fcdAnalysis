# -*- coding: utf-8 -*-
# @Time    : 2018/10/29 17:06
# @Author  : yhdu@tongwoo.cn
# @简介    : 
# @File    : multiMatchData.py

import multiprocessing
from datetime import datetime
from time import clock
import numpy as np
from multi.fcd_processor import match2road
import cx_Oracle


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


def sort_data(trace):
    trace.sort(cmp1)
    last_data = None
    new_trace = []
    for data in trace:
        esti = True
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
            elif dist < 15:  # GPS的误差在10米，不准确
                esti = False
        last_data = data
        if esti:
            new_trace.append(data)
    return new_trace


def read_data(filename):
    """
    从txt里面读取gps数据
    :param filename: 
    :return: 
    """
    bt = clock()
    trace_list = []
    trace = []
    last_veh = ''
    try:
        with open(filename) as fp:
            for line in fp:
                items = line.split(',')
                veh, px, py, stime, state, speed, car_state, ort = items[:]
                px, py, speed, ort = float(px), float(py), float(speed), float(ort)
                state, car_state = int(state), int(car_state)
                stime = datetime.strptime(stime, "%Y-%m-%d %H:%M:%S")
                taxi_data = TaxiData(veh, px, py, stime, state, speed, car_state, ort)
                if last_veh != veh:
                    if len(trace) > 0:
                        trace_list.append(sort_data(trace))
                        trace = []
                trace.append(taxi_data)
                last_veh = veh
            if len(trace) > 0:
                trace_list.append(sort_data(trace))
    except IOError:
        pass

    et = clock()
    print "read data", et - bt
    return trace_list


def sort_work(trace_list, lock, ret_list):
    for trace in trace_list:
        nt = sort_data(trace)
        with lock:
            ret_list.append(nt)


def read_data2(filename):
    bt = clock()
    trace_dict = {}
    with open(filename) as fp:
        for line in fp:
            items = line.split(',')
            veh, px, py, stime, state, speed, car_state, ort = items[:]
            px, py, speed, ort = float(px), float(py), float(speed), float(ort)
            state, car_state = int(state), int(car_state)
            stime = datetime.strptime(stime, "%Y-%m-%d %H:%M:%S")
            taxi_data = TaxiData(veh, px, py, stime, state, speed, car_state, ort)
            try:
                trace_dict[veh].append(taxi_data)
            except KeyError:
                trace_dict[veh] = [taxi_data]
    proc_list = list(trace_dict.values())
    # for veh, trace in trace_dict.iteritems():
    #     trace_list.append(sort_data(trace))
    bt = clock()
    lock = multiprocessing.Lock()
    manager = multiprocessing.Manager()
    trace_list = manager.list()
    pc_list = []
    for i in range(1):
        p = multiprocessing.Process(target=sort_work, args=(proc_list[i::1], lock, trace_list))
        p.daemon = True
        pc_list.append(p)
        p.start()
    for p in pc_list:
        p.join()
    et = clock()
    print "read data2", et - bt
    return trace_list


def match_work(trace_list, ret_list):
    road_temp = {}
    for i in range(24):
        road_temp[i] = {}
    for j, trace in enumerate(trace_list):
        for i, data in enumerate(trace):
            mod_point, cur_edge, speed_list, ret = match2road(data.veh, data, i)
            hour = data.stime.hour
            for edge, spd in speed_list:
                try:
                    road_temp[hour][edge.way_id].append([spd, edge.edge_length])
                except KeyError:
                    road_temp[hour][edge.way_id] = [[spd, edge.edge_length]]

    for hour in range(24):
        for rid, sp_list in road_temp[hour].iteritems():
            W, S = 0, 0
            for sp, w in sp_list:
                S, W = S + sp * w, W + w
            spd = S / W
            n_sample = len(sp_list)
            ret_list.append([rid, spd, n_sample, W, hour])


def multi(begin_time):
    str_dt = begin_time.strftime("%Y-%m-%d")
    print str_dt
    trace_list = read_data("./data/{0}.txt".format(str_dt))
    if len(trace_list) == 0:
        return
    # trace_list = []
    bt = clock()

    manager = multiprocessing.Manager()
    temp_list = manager.list()
    pc_list = []
    for i in range(8):
        p = multiprocessing.Process(target=match_work, args=(trace_list[i::8], temp_list))
        p.daemon = True
        pc_list.append(p)
    for p in pc_list:
        p.start()
    for p in pc_list:
        p.join()
    et = clock()
    print "multi join", et - bt

    # join
    speed_dict = {}
    for h in range(24):
        speed_dict[h] = {}
    for rid, spd, n_sample, w, h in temp_list:
        try:
            speed_dict[h][rid].append([spd, n_sample, w])
        except KeyError:
            speed_dict[h][rid] = [[spd, n_sample, w]]
    speed_list = []
    for h in range(24):
        for rid, info_list in speed_dict[h].iteritems():
            N, S, W = 0, 0, 0
            for spd, n_sample, w in info_list:
                N, S, W = N + n_sample, S + spd * w, W + w
            speed = S / W
            speed_list.append([rid, speed, N, h])
            # print rid, speed, N

    conn = cx_Oracle.connect('hz/hz@192.168.11.88:1521/orcl')
    sql = "insert into tb_history_speed values(:1, :2, :3, :4, :5)"
    # data_hour = begin_time.hour
    data_weekday = begin_time.day
    tup_list = []
    for rid, speed, n_sample, hour in speed_list:
        tup_list.append((rid, speed, hour, data_weekday, n_sample))
    cursor = conn.cursor()
    cursor.executemany(sql, tup_list)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    for _i in range(3, 32):
        st = datetime(2018, 5, _i, 0, 0, 0)
        multi(st)
