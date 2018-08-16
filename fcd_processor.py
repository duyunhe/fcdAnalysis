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
    :return: point, edge, speed_list[edge_index, edge_speed]
    """
    try:
        last_data, last_edge, last_point = data_list[veh], edge_list[veh], point_list[veh]
    except KeyError:
        last_data, last_edge, last_point = None, None, None
    cur_point, cur_edge = mm.PNT_MATCH(data, last_point)

    speed_list = []
    if last_edge is not None and cur_edge is not None:
        dt = (data.stime - last_data.stime).total_seconds()
        dist = calc_dist([data.px, data.py], [last_data.px, last_data.py])
        esti = True
        if dt <= 0 or dt > 90:
            esti = False
        if dist < 10:
            esti = False
        if esti:
            trace, speed_list = estimate_road_speed(last_edge, cur_edge, last_point,
                                                    cur_point, last_data, data, cnt)

    point_list[veh], edge_list[veh] = cur_point, cur_edge
    data_list[veh] = data
    return cur_point, cur_edge, speed_list



