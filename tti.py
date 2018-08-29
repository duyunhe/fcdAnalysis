# -*- coding: utf-8 -*-
# @Time    : 2018/8/22 14:07
# @Author  : 
# @简介    : travel time index 检查道路指数
# @File    : tti.py

from DBConn import oracle_util
import math
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

    tp_list = []
    sql = "delete from tb_road_speed"
    cursor.execute(sql)

    for rid, speed_list in road_speed.iteritems():
        print rid, speed_list
        spd = np.mean(speed_list)
        sql = "insert into tb_road_speed values(:1, :2)"
        tup = (rid, spd)
        cursor.execute(sql, tup)
        tp_list.append(tup)
    # cursor.executemany(sql, tp_list)
    conn.commit()
    conn.close()


def get_tti(radio):
    """
    :param radio: 当前速度与自由流下速度比率
    :return: TTI(travel time index)
    """
    max_radio = 10
    min_radio = 1.2
    if radio > max_radio:
        tti = 10
    elif radio < min_radio:
        tti = 0
    else:
        r1, r0, r = math.log(max_radio), math.log(min_radio), math.log(radio)
        tti = (r - r0) / (r1 - r0) * 10
    return tti
