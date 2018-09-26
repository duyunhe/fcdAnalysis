# -*- coding: utf-8 -*-
# @Time    : 2018/9/4 9:53
# @Author  : 
# @简介    : 
# @File    : history_static.py

from DBConn import oracle_util
import numpy as np

conn = oracle_util.get_connection()
cursor = conn.cursor()
sql = "select * from TB_ROAD_SPEED_PRE order by sta_time, rid"
cursor.execute(sql)
road_speed = {}

for item in cursor:
    try:
        rid = int(item[0])
        speed = float(item[1])
        data_time = item[2]
        hour = data_time.hour
        weekday = data_time.weekday()
    except TypeError:
        print item[1]
        continue
    # flag = 0
    try:
        road_speed[rid][weekday][hour].append(speed)
    except KeyError:
        try:
            road_speed[rid][weekday][hour] = [speed]
        except KeyError:
            try:
                road_speed[rid][weekday] = {hour: [speed]}
            except KeyError:
                road_speed[rid] = {weekday: {hour: [speed]}}

tup_list = []
for rid, d0 in road_speed.iteritems():
    for w, d1 in d0.iteritems():
        for hour, speed_list in d1.iteritems():
            spd = np.mean(speed_list)
            tup_list.append((rid, spd, hour, w))

sql = "insert into tb_history_speed (rid, speed, data_hour, data_weekday) values(:1, :2, :3, :4)"
cursor.executemany(sql, tup_list)
conn.commit()
conn.close()
