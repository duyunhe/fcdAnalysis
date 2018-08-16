# -*- coding: utf-8 -*-
# @Time    : 2018/8/16 16:15
# @Author  : 
# @简介    : 估算速度模块
# @File    : estimate_speed.py


from time import clock
from geo import calc_dist, point_project_edge
from map_struct import DistNode
import Queue
import numpy as np


def get_speed_list(travel_list, last_spd, cur_spd, ave_spd, itv_time):
    """
    返回各个路段上估算的速度列表
    :param travel_list: 路段和距离列表 [[edge, dist] ..]
    :param last_spd: 
    :param cur_spd: 
    :param ave_spd: 
    :param itv_time: 间隔时间，秒
    :return: seg_speed_list 路段和速度列表 [[edge, speed], ..]
    """
    # 首先，异常情况
    abnormal = True
    if last_spd <= ave_spd <= cur_spd:
        abnormal = False
    if cur_spd <= ave_spd <= last_spd:
        abnormal = False
    if abnormal:
        speed_list = [ave_spd] * len(travel_list)
        edge_list, dist_list = zip(*travel_list)
        seg_speed_list = zip(edge_list, speed_list)
        return seg_speed_list

    # 假设浮动车在端点上的速度沿着长度做线性变化
    temp = [last_spd]
    L = 0.0  # total dist
    for edge, dist in travel_list:
        L += dist
    T = itv_time
    v0 = last_spd
    dv = cur_spd - last_spd
    l = 0.0
    for i, travel in enumerate(travel_list):
        if i == len(travel_list) - 1:
            break
        edge, dist = travel[0:2]
        l += dist
        v = l / L * dv + v0
        temp.append(v)
    temp.append(cur_spd)
    speed_list = []
    for i, v in enumerate(temp):
        if i == 0:
            continue
        speed_list.append((temp[i] + temp[i - 1]) / 2)

    # compare to reality
    t = 0.0
    i = 0
    for edge, dist in travel_list:
        t += dist / speed_list[i] * 3.6
        i += 1
    # modify index
    alpha = t / itv_time
    # 最后拼接
    speed_list = [v * alpha for v in speed_list]
    edge_list, dist_list = zip(*travel_list)
    seg_speed_list = zip(edge_list, speed_list)

    return seg_speed_list


def init_candidate_queue(last_point, last_edge, can_queue, node_set):
    """
    initialize the queue, add one or two points of the last edge
    """
    _, ac, state = point_project_edge(last_point, last_edge)
    project_dist = np.linalg.norm(np.array(ac))

    # 投影点到线段两端点的距离
    dist0, dist1 = project_dist, last_edge.edge_length - project_dist
    # 投影点在线段延长线上，此处TODO
    if dist0 > last_edge.edge_length:
        dist0, dist1 = last_edge.edge_length, 0

    if last_edge.oneway:
        node = last_edge.node1
        dnode = DistNode(node, dist1)
        can_queue.put(dnode)
    else:
        node = last_edge.node0
        dnode = DistNode(node, dist0)
        can_queue.put(dnode)
        node_set.add(node.nodeid)

        node = last_edge.node1
        dnode = DistNode(node, dist1)
        can_queue.put(dnode)

    node_set.add(node.nodeid)


def estimate_road_speed(last_edge, cur_edge, last_point, cur_point, last_data, cur_data, cnt=-1):
    """
    通过路网估计两条数据之间各个路段的速度
    :param last_edge: 上一条匹配边
    :param cur_edge: 当前匹配的边
    :param last_point: 上一个匹配到的GPS点
    :param cur_point: 当前匹配到的GPS点
    :param last_data: 上一次记录的GPS数据
    :param cur_data: 本次记录的GPS数据
    :param cnt:     for debug
    :return: trace, travel list : travel data:[edge_index, edge_speed]
    """
    bt = clock()
    spoint, epoint = last_point, cur_point
    # 首先是同一条边的情况
    if last_edge == cur_edge:
        cur_dist = calc_dist(spoint, epoint)
        itv_time = (cur_data.stime - last_data.stime).total_seconds()
        # 就这样简单粗暴
        observed_spd = cur_dist / itv_time * 3.6
        return [spoint, epoint], [[cur_edge, observed_spd]]

    # 使用Dijkstra算法搜索路径
    # 加入优先队列(最小堆)优化速度
    # 基准测试平均每次执行速度在万一秒左右 0.0001s，主要因为道路拓扑不是很复杂
    # 理论上应该用A*的H来做优先队列value函数
    # 并将H超过cur_dist的剪枝，不过在道路拓扑并不复杂的情况下，这个剪枝效率能提升多少存疑
    node_set = set()
    q = Queue.PriorityQueue(maxsize=-1)
    init_candidate_queue(last_point, last_edge, q, node_set)

    edge_found = False
    while not q.empty() and not edge_found:
        dnode = q.get()
        cur_node, cur_dist = dnode.node, dnode.dist
        if cur_dist > 500:
            break
        # 宽度优先
        for edge, node in cur_node.link_list:
            if node.nodeid in node_set:
                continue
            node_set.add(node.nodeid)
            next_dnode = DistNode(node, cur_dist + edge.edge_length)
            # 在每个MapNode里面记录下搜索路径
            node.prev_node, node.prev_edge = cur_node, edge
            q.put(next_dnode)
            if edge.edge_index == cur_edge.edge_index:
                edge_found = True

    if not edge_found:
        return [], []

    # 根据纪录重建路径
    # 终点
    trace = [cur_point]
    n0, n1 = cur_edge.node0, cur_edge.node1
    if n0.prev_node == n1:  # n1 is nearer from last point
        cur_node = n1
    else:
        cur_node = n0
    dist = calc_dist(cur_point, cur_node.point)
    travel = [[cur_edge, dist]]  # travel edge list, from last edge to current edge

    # 逆推
    while cur_node != last_edge.node0 and cur_node != last_edge.node1:
        trace.append(cur_node.point)
        prev_edge = cur_node.prev_edge
        travel.append([prev_edge, prev_edge.edge_length])
        cur_node = cur_node.prev_node

    # 起点
    dist = calc_dist(last_point, cur_node.point)
    travel.append([last_edge, dist])
    # 所以要倒序
    travel = travel[::-1]

    travel_dist = 0.0
    # 简单加一下
    for edge, dist in travel:
        travel_dist += dist
    itv_time = (cur_data.stime - last_data.stime).total_seconds()

    # 得到如下值
    # 1.观测得到的行驶距离，行驶时间，可以得到平均速度
    observed_spd = travel_dist / itv_time * 3.6
    # 2.之前一个点的速度 当前点的速度
    last_spd, cur_spd = last_data.speed, cur_data.speed
    # 3.行驶各路段
    trace.append(cur_node.point)
    trace.append(spoint)
    et = clock()
    # print "estimate,{0} ".format(cnt), et - bt
    spd_list = get_speed_list(travel, last_spd, cur_spd, observed_spd, itv_time)

    return trace, spd_list
