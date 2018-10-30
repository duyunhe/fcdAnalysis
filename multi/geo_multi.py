# -*- coding: utf-8 -*-
# @Time    : 2018/10/30 9:45
# @Author  : yhdu@tongwoo.cn
# @简介    : 
# @File    : geo_multi.py

import math
import numpy as np


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


def calc_included_segment(pt, p0, p1):
    return calc_included_angle(pt, p0, pt, p1)


def calc_included_angle(s0p0, s0p1, s1p0, s1p1):
    """
    计算夹角
    :param s0p0: 线段0点0 其中点用[x,y]表示
    :param s0p1: 线段0点1 
    :param s1p0: 线段1点0
    :param s1p1: 线段1点1
    :return: 
    """
    # numpy比math慢50%左右
    # v0 = np.array([s0p1[0] - s0p0[0], s0p1[1] - s0p0[1]])
    # v1 = np.array([s1p1[0] - s1p0[0], s1p1[1] - s1p0[1]])
    # dt = np.sqrt(np.dot(v0, v0)) * np.sqrt(np.dot(v1, v1))
    # if dt == 0:
    #     return 0
    # ret = np.dot(v0, v1) / dt
    # print ret

    ax, ay = s0p1[0] - s0p0[0], s0p1[1] - s0p0[1]
    bx, by = s1p1[0] - s1p0[0], s1p1[1] - s1p0[1]
    dt = math.sqrt((ax * ax + ay * ay)) * math.sqrt((bx * bx + by * by))
    if dt == 0:
        return 0
    ret = (ax * bx + ay * by) / dt
    return ret


def is_near_segment(pt0, pt1, pt2, pt3):
    v0 = np.array([pt1[0] - pt0[0], pt1[1] - pt0[1]])
    v1 = np.array([pt3[0] - pt2[0], pt3[1] - pt2[1]])
    dt = np.sqrt(np.dot(v0, v0)) * np.sqrt(np.dot(v1, v1))
    if dt == 0:
        return False
    ret = np.dot(v0, v1) / dt > math.cos(np.pi / 1.5)
    return ret


def get_eps(x0, y0, x1, y1):
    # calculate arctan(dy / dx)
    dx, dy = x1 - x0, y1 - y0
    # angle = angle * 180 / np.pi
    if np.fabs(dx) < 1e-10:
        if y1 > y0:
            return 90
        else:
            return -90
    angle = math.atan2(dy, dx)
    angle2 = angle * 180 / np.pi
    return angle2


def get_diff(e0, e1):
    # 计算夹角，取pi/2到-pi/2区间的绝对值
    de = e1 - e0
    if de >= 180:
        de -= 360
    elif de < -180:
        de += 360
    return math.fabs(de)


def get_guass_proc(dist):
    sig = 20.0
    return 1.0 / (math.sqrt(2.0 * math.pi) * sig) * math.exp(-(dist * dist) / (2.0 * sig * sig))


def point_project_edge(point, edge):
    n0, n1 = edge.node0, edge.node1
    sp0, sp1 = n0.point, n1.point
    return point_project(point, sp0, sp1)


def point_project(point, segment_point0, segment_point1):
    """
    :param point: point to be matched
    :param segment_point0: segment
    :param segment_point1: 
    :return: projected point, ac, state
            state 为1 在s0s1的延长线上  
            state 为-1 在s1s0的延长线上
    """
    x, y = point[0:2]
    x0, y0 = segment_point0[0:2]
    x1, y1 = segment_point1[0:2]
    ap, ab = np.array([x - x0, y - y0]), np.array([x1 - x0, y1 - y0])
    ac = np.dot(ap, ab) / (np.dot(ab, ab)) * ab
    dx, dy = ac[0] + x0, ac[1] + y0
    state = 0
    if np.dot(ap, ab) < 0:
        state = -1
    bp, ba = np.array([x - x1, y - y1]), np.array([x0 - x1, y0 - y1])
    if np.dot(bp, ba) < 0:
        state = 1
    return [dx, dy], ac, state


def point2segment(point, segment_point0, segment_point1):
    """

    :param point: point to be matched
    :param segment_point0: segment
    :param segment_point1: 
    :return: dist from point to segment
    """
    x, y = point[0:2]
    x0, y0 = segment_point0[0:2]
    x1, y1 = segment_point1[0:2]
    cr = (x1 - x0) * (x - x0) + (y1 - y0) * (y - y0)
    if cr <= 0:
        return math.sqrt((x - x0) * (x - x0) + (y - y0) * (y - y0))
    d2 = (x1 - x0) * (x1 - x0) + (y1 - y0) * (y1 - y0)
    if cr >= d2:
        return math.sqrt((x - x1) * (x - x1) + (y - y1) * (y - y1))
    r = cr / d2
    px = x0 + (x1 - x0) * r
    py = y0 + (y1 - y0) * r
    return math.sqrt((x - px) * (x - px) + (y - py) * (y - py))


def draw_raw(traj, ax):
    xlist, ylist = [], []
    for point in traj:
        xlist.append(point.px)
        ylist.append(point.py)
    ax.plot(xlist, ylist, marker='o', linestyle='--', color='k', lw=1)



def transformlat(lng, lat):
    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * math.pi) + 20.0 *
            math.sin(2.0 * lng * math.pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * math.pi) + 40.0 *
            math.sin(lat / 3.0 * math.pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * math.pi) + 320 *
            math.sin(lat * math.pi / 30.0)) * 2.0 / 3.0
    return ret


def transformlng(lng, lat):
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
        0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * math.pi) + 20.0 *
            math.sin(2.0 * lng * math.pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * math.pi) + 40.0 *
            math.sin(lng / 3.0 * math.pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * math.pi) + 300.0 *
            math.sin(lng / 30.0 * math.pi)) * 2.0 / 3.0
    return ret