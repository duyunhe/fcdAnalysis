# -*- coding: utf-8 -*-
# @Time    : 2018/8/22 14:07
# @Author  : 
# @简介    : travel time index 检查道路指数
# @File    : tti.py

from DBConn import oracle_util
import numpy as np


def calc_speed():
    conn = oracle_util.get_connection()
    sql = "select * from tb_history_speed"
    cursor = conn.cursor()
    cursor.execute(sql)
    road_speed = {}
    for item in cursor:
        rid = int(item[0])
        speed = float(item[1])
        try:
            road_speed[rid].append(speed)
        except KeyError:
            road_speed[rid] = [speed]
    sql = "update tb_road_state set def_speed = :1 where rid = :2"
    tp_list = []
    for rid, speed_list in road_speed.iteritems():
        print rid, speed_list
        spd = np.mean(speed_list)
        tup = (rid, spd)
        tp_list.append(tup)
    cursor.executemany(sql, tp_list)
    conn.commit()


calc_speed()
