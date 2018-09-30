# -*- coding: utf-8 -*-
# @Time    : 2018/8/22 14:07
# @Author  : 
# @简介    : travel time index 检查道路指数
# @File    : tti.py

from DBConn import oracle_util
import math
import numpy as np
import matplotlib.pyplot as plt


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


def get_tti_v2(speed, def_speed):
    """
    storm高速公路实时交通指数评估方法中的tti实现
    划分为五档
    畅通 大于自由流速度的0.7倍
    基本畅通 在0.5到0.7之间
    轻度拥堵 在0.4到0.5之间
    中度拥堵 在0.3到0.4之间
    严重拥堵 在0到0.3之间
    :param speed: 路段实际速度
    :param def_speed: 路段期望速度
    :return: 
    """
    v_list = [1.0, 0.7, 0.5, 0.4, 0.3, 0, -1e20]
    radio = speed / def_speed
    if radio > 1.0:
        return 0.0
    if radio <= 0.0:
        return 10.0
    for i, v in enumerate(v_list):
        if v_list[i + 1] < radio <= v_list[i]:
            tti = (i + 1) * 2 - (radio - v_list[i + 1]) / (v_list[i] - v_list[i + 1]) * 2
            return tti


def get_tti_v1(speed, def_speed):
    """
    :param speed: 路段实际速度
    :param def_speed: 路段期望速度
    :return: tti
    """
    radio = def_speed / speed
    max_radio = def_speed / 5.0     # 严重拥堵情况下的最大比率
    min_radio = 1.0
    if radio > max_radio:
        tti = 9.9
    elif radio < min_radio:
        tti = 0
    else:
        tti = (radio - min_radio) / (max_radio - min_radio) * 10
    return tti


def get_tti_v0(speed, def_speed):
    """
    :param speed: 路段实际速度
    :param def_speed: 路段期望速度
    :return: tti
    """
    if speed < 1e-5:
        speed = 0.1
    radio = def_speed / speed
    max_radio = 4.0
    min_radio = 1.0
    if radio > max_radio:
        tti = 9.9
    elif radio < min_radio:
        tti = 0
    else:
        tti = (radio - min_radio) / (max_radio - min_radio) * 10
    return tti


def draw():
    x = np.arange(0, 1, 0.01)
    y = [get_tti_v2(i, 1) for i in x]
    plt.plot(x, y)
    plt.show()

