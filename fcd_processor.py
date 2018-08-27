# -*- coding: utf-8 -*-
# @Time    : 2018/8/16 10:58
# @Author  : 
# @简介    : 浮动车控制模块，保存信息，实现功能
# @File    : fcd_processor.py

from map_matching import MapMatching
from geo import calc_dist
from estimate_speed import estimate_road_speed


edge_list, point_list, data_list = {}, {}, {}
mm = MapMatching()
# 根据车辆id保存车辆各种信息


def match2road(veh, data, cnt):
    """
    :param veh: 车辆id
    :param data: TaxiData，见map_matching
    :param cnt:  for debug
    :return: point, edge, speed_list[edge, edge_speed], state
    """
    try:
        last_data, last_edge, last_point = data_list[veh], edge_list[veh], point_list[veh]
        dist = calc_dist([data.px, data.py], [last_data.px, last_data.py])
    except KeyError:
        last_data, last_edge, last_point, dist = None, None, None, None

    # if veh == 'AT3006':
    #     print "data", cnt, data.speed, data.stime, dist
    cur_point, cur_edge = mm.PNT_MATCH(data, last_data, last_point, cnt)
    # print "process {0}".format(cnt)
    speed_list = []
    ret = -1
    esti = True
    if last_data is not None:
        dt = (data.stime - last_data.stime).total_seconds()
        # 过滤异常
        if dt <= 10 or dt > 120:
            esti = False
        elif dist > 100 / 3.6 * dt:  # 距离阈值
            esti = False
        elif data.speed == last_data.speed and data.direction == last_data.direction:  # 非精确
            esti = False
        elif dist < 15:             # GPS的误差在15米，不准确
            esti = False
    if cnt == 0:
        esti = False
    if not esti:
        point_list[veh], edge_list[veh] = cur_point, cur_edge
        data_list[veh] = data
        return cur_point, cur_edge, speed_list, ret

    # 两种情况
    if last_edge is not None and cur_edge is not None:
        trace, speed_list = estimate_road_speed(last_edge, cur_edge, last_point,
                                                cur_point, last_data, data, cnt)
        for edge, spd in speed_list:
            if edge.way_id == 1000045:
                print 'suc', veh, spd, data.stime
        ret = 0
    elif last_edge is None and cur_edge is not None:
        speed_list = [[cur_edge, data.speed]]
        for edge, spd in speed_list:
            if edge.way_id == 1000045:
                print 'first', veh, spd, data.stime, dist
        ret = 1

    point_list[veh], edge_list[veh] = cur_point, cur_edge
    data_list[veh] = data
    return cur_point, cur_edge, speed_list, ret


def draw_map(road_speed):
    mm.plot_map(road_speed)



