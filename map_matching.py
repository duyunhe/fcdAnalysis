# coding=utf-8
from xml.etree import ElementTree as ET
import matplotlib.pyplot as plt
from sklearn.neighbors import KDTree
import math
import Queue
from map_struct import DistNode, MapEdge, MapNode, MatchResult
from geo import point2segment, point_project, calc_dist, bl2xy, is_near_segment, \
    calc_included_angle, point_project_edge, get_guass_proc
from time import clock
import numpy as np


color = ['r-', 'b-', 'g-', 'c-', 'm-', 'y-', 'c-', 'r-', 'b-', 'orchid', 'm--', 'y--', 'c--', 'k--', 'r:']
region = {'primary': 0, 'secondary': 1, 'tertiary': 2,
          'unclassified': 5, 'trunk': 3, 'service': 4, 'trunk_link': 6,
          'primary_link': 7, 'secondary_link': 8}

EDGE_ONEWAY = 3
EDGES = 2
EDGE_INDEX = 4
EDGE_LENGTH = 5
NODE_EDGELIST = 2

map_node = {}
map_edge = []
map_way = {}
# global data structure
nodeid_list = []


def edge2xy(e):
    x0, y0 = e.node0.point[0:2]
    x1, y1 = e.node1.point[0:2]
    return x0, y0, x1, y1


def draw_map():
    for i in map_way:
        pl = map_way[i]
        node_list = pl['node']
        x, y = [], []
        for node in node_list:
            x.append(node.point[0])
            y.append(node.point[1])

        try:
            c = color[region[pl['highway']]]
            plt.plot(x, y, c, alpha=0.3)
        except KeyError:
            continue
        # if 'name' in pl:
        #     name = pl['name']
        #     plt.text(x[0] + 10, y[0] + 10, name)


def draw_seg(seg, c):
    x, y = zip(*seg)
    plt.plot(x, y, c, linewidth=2)


def draw_edge_set(edge, edge_set, node):
    for i in edge_set:
        draw_edge(edge[i], 'b')


def draw_edge(e, c):
    x0, y0, x1, y1 = edge2xy(e)
    x, y = [x0, x1], [y0, y1]
    plt.plot(x, y, c, linewidth=3)
    plt.text((x[0] + x[-1]) / 2, (y[0] + y[-1]) / 2, '{0}'.format(e.edge_index))


def draw_edge_list(edge_list):
    for edge in edge_list:
        if edge.oneway is True:
            draw_edge(edge, 'gold')
        else:
            draw_edge(edge, 'brown')


def draw_nodes(node_list):
    x, y = [], []
    for node in node_list:
        x.append(node[0])
        y.append(node[1])
    plt.plot(x, y, 'mo', markersize=5)


def draw_points(points):
    x, y = zip(*points)
    plt.plot(x, y, 'ro', markersize=4)


def draw_mod_results(results):
    x, y = [], []
    xsel, ysel = [], []
    for j, r in enumerate(results):
        sel = r.sel
        for i, match_point in enumerate(r.match_point_list):
            mp, edge_index, score = match_point.mod_point, match_point.edge_index, match_point.score
            # if j == 2:
            #     plt.text(mp[0], mp[1], "{0}".format(score))
            if i == sel:
                xsel.append(mp[0])
                ysel.append(mp[1])
            else:
                x.append(mp[0])
                y.append(mp[1])
    plt.plot(xsel, ysel, 'ro', markersize=4)
    plt.plot(x, y, 'co', markersize=3)


def draw_point(point, c):
    """
    :param point: [x, y]
    :return: 
    """
    plt.plot([point[0]], [point[1]], c, markersize=6)


def store_link():
    for edge in map_edge:
        n0, n1 = edge.node0, edge.node1
        if edge.oneway is True:
            n0.add_link(edge, n1)
            n1.add_rlink(edge, n0)
        else:
            n0.add_link(edge, n1)
            n1.add_link(edge, n0)
            n0.add_rlink(edge, n1)
            n1.add_rlink(edge, n0)


def store_node(tree):
    p = tree.find('meta')
    nds = p.findall('node')
    for x in nds:
        node_dic = x.attrib
        nodeid = node_dic['id']
        dx, dy = bl2xy(float(node_dic['lat']), float(node_dic['lon']))
        node = MapNode([dx, dy], nodeid)
        map_node[nodeid] = node


def store_edge(tree):
    p = tree.find('meta')
    wys = p.findall('way')
    for w in wys:
        way_dic = w.attrib
        wid = way_dic['id']
        node_list = w.findall('nd')
        map_way[wid] = {}
        oneway = False
        ref = map_way[wid]
        tag_list = w.findall('tag')
        for tag in tag_list:
            tag_dic = tag.attrib
            ref[tag_dic['k']] = tag_dic['v']
        if 'oneway' in ref:
            oneway = ref['oneway'] == 'yes'

        node_in_way = []
        for nd in node_list:
            node_dic = nd.attrib
            node_in_way.append(map_node[node_dic['ref']])
        ref['node'] = node_in_way
        last_node = None
        ref['edge'] = []
        for node in node_in_way:
            if last_node is not None:
                edge_index = len(map_edge)
                ref['edge'].append(edge_index)
                p0, p1 = last_node.point, node.point
                edge_length = calc_dist(p0, p1)
                edge = MapEdge(last_node, node, oneway, edge_index, edge_length, wid)
                map_edge.append(edge)
            last_node = node


def calc_best_path(mr_list):
    """
    根据得分计算最优路径
    以最后一个点的得分作为判定，回溯至起始点
    :param mr_list: 
    :return: 
    """
    n = len(mr_list)
    score = []
    for i in range(n):
        score.append({})
    last_score = 0      # 记录上一个点匹配中最大的得分值

    for i, mr in enumerate(mr_list):
        if mr.first is True:
            for mp in mr.match_point_list:
                ei, s = mp.edge_index, mp.score
                score[i][ei] = [s + last_score, -1, None]
        else:
            last_score = 1e20
            for mp in mr.match_point_list:
                mod_point, edge, s, dist = mp.mod_point, map_edge[mp.edge_index], mp.score, mp.dist
                for last_edge_index in mp.last_index_list:
                    try:
                        new_score = s + score[i - 1][last_edge_index][0]
                        last_score = min(last_score, new_score)
                        if edge.edge_index not in score[i]:
                            score[i][edge.edge_index] = [new_score, last_edge_index, mod_point]
                        else:
                            if new_score < score[i][edge.edge_index][0]:
                                score[i][edge.edge_index] = [new_score, last_edge_index, mod_point]
                    except KeyError:
                        print i

    # 绘制
    last_edge_index, edge_index = -1, -1
    last_first = True
    for i in range(n - 1, 0, -1):
        if last_first:
            # 计算最后的最大得分，然后往前迭代
            min_score, mod_point = 1e20, None
            # 最小距离，当前边，上一次匹配的边，当前匹配点
            for ei, v in score[i].iteritems():
                s, last_ei, mp = v[0:3]
                if s < min_score:
                    last_edge_index, min_score, edge_index, mod_point = last_ei, s, ei, mp
            cur_edge, last_edge = map_edge[edge_index], map_edge[last_edge_index]
            # 标点
            mp_list = mr_list[i].match_point_list
            for j, mp in enumerate(mp_list):
                if edge_index == mp.edge_index:
                    mr_list[i].set_sel(j)
                    break
        else:
            try:
                last_edge_index = score[i][edge_index][1]
                mod_point = score[i][edge_index][2]
                cur_edge, last_edge = map_edge[edge_index], map_edge[last_edge_index]
                mp_list = mr_list[i].match_point_list
                for j, mp in enumerate(mp_list):
                    if edge_index == mp.edge_index:
                        mr_list[i].set_sel(j)
                        break
            except KeyError:
                print i
        edge_index = last_edge_index
        last_point = mr_list[i - 1].point
        trace, _ = get_trace_dyn(last_edge, cur_edge, last_point, mod_point, i)
        last_first = mr_list[i].first
        try:
            draw_seg(trace, 'b')
        except TypeError:
            print 'trace', i


def calc_node_dict(node):
    """
    dijkstra算法计算最短路径
    保存在node中dist字典内
    :param node: MapNode
    :return: null
    """
    T = 80000 / 3600 * 10   # dist_thread
    node_set = set()        # node_set用于判断是否访问过
    edge_set = set()        # edge_set用于记录能够访问到的边
    q = Queue.PriorityQueue(maxsize=-1)     # 优先队列优化
    # initialize
    init_node = DistNode(node.nodeid, 0)
    node_set.add(node.nodeid)
    q.put(init_node)
    # best first search
    while not q.empty():
        cur_node = q.get()
        if cur_node.dist > T:
            break
        for edge, nextid in map_node[cur_node.nodeid].link_list:
            edge_set.add(edge.edge_index)
            if nextid in node_set:
                continue
            node_set.add(nextid)
            new_node = DistNode(nextid, cur_node.dist + edge.edge_length)
            node.dist_dict[nextid] = new_node.dist
            q.put(new_node)

    # store edge indexes which can reach
    node.reach_set = edge_set


def read_xml(filename):
    t = clock()
    tree = ET.parse(filename)
    store_node(tree)
    store_edge(tree)
    store_link()
    print 'load map', clock() - t


def make_kdtree():
    nd_list = []
    for key, item in map_node.items():
        nodeid_list.append(key)
        nd_list.append(item.point)
    X = np.array(nd_list)
    return KDTree(X, leaf_size=2, metric="euclidean"), X


def get_candidate_first(taxi_data, kdt, X):
    """
    get candidate edges from road network which fit point 
    :param taxi_data: Taxi_Data  .px, .py, .speed, .stime
    :param kdt: kd-tree
    :return: edge candidate list  list[edge0, edge1, edge...]
    """
    dist, ind = kdt.query([[taxi_data.px, taxi_data.py]], k=50)

    pts = []
    seg_set = set()
    # fetch nearest map nodes in network around point, then check their linked edges
    for i in ind[0]:
        pts.append([X[i][0], X[i][1]])
        node_id = nodeid_list[i]
        edge_list = map_node[node_id].link_list
        for e, nd in edge_list:
            seg_set.add(e.edge_index)
        # here, need reverse link,
        # for its first node can be far far away, then this edge will not be included
        edge_list = map_node[node_id].rlink_list
        for e, nd in edge_list:
            seg_set.add(e.edge_index)

    edge_can_list = []
    for i in seg_set:
        edge_can_list.append(map_edge[i])

    return edge_can_list


def init_candidate_queue(last_point, last_edge, can_queue, node_set):
    """
    initialize the queue, add one or two points of the last edge
    """
    _, ac, state = point_project_edge(last_point, last_edge)
    project_dist = np.linalg.norm(np.array(ac))
    dist0, dist1 = project_dist, last_edge.edge_length - project_dist
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


def get_candidate_later(cur_data, last_data, last_point, last_edge, last_state, itv_time, cnt):
    """
    :param cur_data: current taxi_data
    :param last_data: last taxi_data
    :param last_point 
    :param last_edge: MapEdge
    :param last_state: direction of vehicle in map edge
    :return: edge_can_list [edge0, edge1....]
    """
    edge_can_list = []
    node_set = set()                        # node_set用于判断是否访问过
    edge_set = set()                        # edge_set用于记录能够访问到的边
    cur_point = [cur_data.px, cur_data.py]

    cart_dist = calc_dist(cur_point, last_point)
    eva_speed = max(cur_data.speed, last_data.speed) / 1.8
    if itv_time > 60:
        T = min(2.0 * cart_dist, 60 / 3.6 * itv_time)
    else:
        T = max(3.0 * cart_dist, eva_speed * itv_time)  # dist_thread
    # print 'thread', cnt, T, eva_speed, cart_dist, itv_time

    if last_edge.oneway is False or is_near_segment(last_point, cur_point,
                                                    last_edge.node0.point, last_edge.node1.point):
        edge_set.add(last_edge.edge_index)

    q = Queue.PriorityQueue(maxsize=-1)     # 优先队列 best first search
    init_candidate_queue(last_point, last_edge, q, node_set)    # 搜索第一步，加入之前线段中的点

    while not q.empty():
        dnode = q.get()
        cur_node, cur_dist = dnode.node, dnode.dist
        if cur_dist >= T:       # 超过阈值后停止
            break
        for edge, node in cur_node.link_list:
            if node.nodeid in node_set:
                continue
            node_set.add(node.nodeid)
            # 单行线需要判断角度
            if edge.oneway is False or is_near_segment(last_point, cur_point, edge.node0.point, edge.node1.point):
                edge_set.add(edge.edge_index)
            next_dnode = DistNode(node, cur_dist + edge.edge_length)
            node.prev_node = cur_node
            q.put(next_dnode)

    for i in edge_set:
        edge = map_edge[i]
        dist = point2segment(cur_point, edge.node0.point, edge.node1.point)
        if dist < 60:
            edge_can_list.append(map_edge[i])

    return edge_can_list


def _get_mod_point_first(candidate, point):
    """
    :param candidate: 
    :param point: current point
    :return: project_point, sel_edge
    """
    min_dist, sel_edge = 1e20, None

    # first point
    for edge in candidate:
        # n0, n1 = edge.node0, edge.nodeid1
        p0, p1 = edge.node0.point, edge.node1.point
        dist = point2segment(point, p0, p1)
        if min_dist > dist:
            min_dist, sel_edge = dist, edge

    sel_node0, sel_node1 = sel_edge.node0, sel_edge.node1
    project_point, _, state = point_project(point, sel_node0.point, sel_node1.point)
    # print sel_edge.edge_index, min_dist
    return project_point, sel_edge, min_dist


def _get_mod_point_later(candidate, point, last_point, cnt):
    """
    :param candidate: 
    :param point: current position point
    :param last_point: last position point
    :return: project_point, sel_edge, score
    """
    min_score, sel_edge = 1e10, None

    for edge in candidate:
        p0, p1 = edge.node0.point, edge.node1.point
        w0, w1 = 1.0, 10.0
        # 加权计算分数，考虑夹角的影响
        dist = point2segment(point, p0, p1)
        angle = calc_included_angle(last_point, point, p0, p1)
        if not edge.oneway and angle < 0:
            angle = -angle
        score = w0 * dist + w1 * (1 - angle)
        if score < min_score:
            min_score, sel_edge = score, edge
        # if cnt == 147:
        #     print edge.edge_index, dist, score, angle

    if sel_edge is None:
        return None, None, 0
    project_point, _, state = point_project(point, sel_edge.node0.point, sel_edge.node1.point)
    if state == 1:
        # 点落在线段末端外
        project_point = sel_edge.node1.point
    elif state == -1:
        project_point = sel_edge.node0.point
    return project_point, sel_edge, min_score


def get_score(point, last_point, cur_edge):
    """
    判定得分
    :param point: 
    :param last_point: 
    :param cur_edge: 
    :return: 
    """
    p0, p1 = cur_edge.node0.point, cur_edge.node1.point
    dist = point2segment(point, p0, p1)
    angle = calc_included_angle(last_point, point, p0, p1)
    if not cur_edge.oneway and angle < 0:
        angle = -angle
    w0, w1 = 1.0, 10.0
    score = w0 * dist + w1 * (1 - angle)
    if math.fabs(angle) < math.cos(math.pi * 2 / 3):
        score += 500
    return score, dist


def get_st_score(point, last_point, cur_edge, last_edge):
    _, trace_dist = get_trace_dyn(last_edge, cur_edge, last_point, point)
    p0, p1 = cur_edge.node0.point, cur_edge.node1.point
    dist = point2segment(point, p0, p1)
    dir_dist = calc_dist(last_point, point)
    pro_d = get_guass_proc(dist)
    pro_f = dir_dist / dist
    score = -math.log(pro_d * pro_f + 1)
    return score, dist


def get_mod_points(taxi_data, candidate, last_point, last_edge, cnt=-1):
    """
    get all fit points
    :param taxi_data: 
    :param candidate: 
    :param last_point: last gps point
    :param last_edge: last matched edge
    :param cnt: 
    :return: edge, mod_point, dist, score
    """
    bt = clock()
    point = [taxi_data.px, taxi_data.py]
    edge_list, mod_list, dist_list, score_list = [], [], [], []

    for edge in candidate:
        score, dist = get_score(point, last_point, edge)
        # s2, d2 = get_st_score(point, last_point, edge, last_edge)
        edge_list.append(edge)
        dist_list.append(dist)
        score_list.append(score)

    # 以下代码作废
    state_list = []
    in_road = False
    for edge in edge_list:
        project_point, _, state = point_project(point, edge.node0.point, edge.node1.point)
        if state == 1:
            # 点落在线段末端外
            project_point = edge.node1.point
        elif state == -1:
            project_point = edge.node0.point
        else:
            in_road = True
        mod_list.append(project_point)
        state_list.append(state)

    temp = zip(mod_list, edge_list, dist_list, score_list)
    match_list = []
    for i, mtc in enumerate(temp):
        # state = state_list[i]
        # if in_road and state != 0:
        #     continue
        match_list.append(mtc)

    return match_list


def get_mod_point(taxi_data, candidate, last_point, cnt=-1):
    """
    get best fit point matched with candidate edges
    :param taxi_data: Taxi_Data
    :param candidate: list[edge0, edge1, edge...]
    :param last_point: last matched point
    :param cnt: for debug
    :return: matched point, matched edge, minimum distance from point to matched edge
    """
    point = [taxi_data.px, taxi_data.py]
    if last_point is None:
        # 第一个点
        return _get_mod_point_first(candidate, point)
    else:
        return _get_mod_point_later(candidate, point, last_point, cnt)


def get_first_point(point, kdt, X):
    """
    match point to nearest segment
    :param point: point to be matched
    :param kdt: kdtree
    :param X: 
    :return: 
    """
    dist, ind = kdt.query([point], k=30)

    pts = []
    seg_set = set()
    for i in ind[0]:
        pts.append([X[i][0], X[i][1]])
        node_id = nodeid_list[i]
        edge_list = map_node[node_id].link_list
        for e, nd in edge_list:
            seg_set.add(e.edge_index)

    min_dist, sel = 1e20, -1
    for idx in seg_set:
        n0, n1 = map_edge[idx].nodeid0, map_edge[idx].nodeid1
        p0, p1 = map_node[n0].point, map_node[n1].point
        dist = point2segment(point, p0, p1)
        if min_dist > dist:
            min_dist, sel = dist, idx

    sel_edge = map_edge[sel]
    sel_node0, sel_node1 = sel_edge.nodeid0, sel_edge.nodeid1
    x0, y0 = map_node[sel_node0].point[0:2]
    x1, y1 = map_node[sel_node1].point[0:2]
    x, y = point[0:2]
    rx, ry, _ = point_project(x, y, x0, y0, x1, y1)
    return rx, ry, sel_edge


def get_mod_points0(traj_order):
    """
    White00 algorithm 1, basic algorithm point to point
    """
    kdt, X = make_kdtree()
    traj_mod = []
    # traj_point: [x, y]
    for taxi_data in traj_order:
        px, py, last_edge = get_first_point([taxi_data.px, taxi_data.py], kdt=kdt, X=X)
        traj_mod.append([px, py])

    return traj_mod


def get_trace_dist(trace):
    last_point = None
    trace_dist = 0
    for point in trace:
        if last_point is not None:
            dist = calc_dist(point, last_point)
            trace_dist += dist
        last_point = point
    return trace_dist


def get_trace_dyn(last_edge, cur_edge, last_point, cur_point, cnt=-1):
    """
    通过路网计算两点之间的路径
    :param last_edge: 上一条匹配边
    :param cur_edge: 当前匹配的边
    :param last_point: 上一个GPS点
    :param cur_point: 当前GPS点
    :return: trace, dist of trace
    """
    spoint, _, _ = point_project_edge(last_point, last_edge)
    try:
        epoint, _, _ = point_project_edge(cur_point, cur_edge)
    except TypeError:
        print 'get_trace_dyn', cnt
    if last_edge == cur_edge:
        return [spoint, epoint], calc_dist(spoint, epoint)

    node_set = set()
    q = Queue.PriorityQueue(maxsize=-1)
    init_candidate_queue(last_point, last_edge, q, node_set)

    # 使用Dijkstra算法搜索路径
    edge_found = False
    while not q.empty() and not edge_found:
        dnode = q.get()
        cur_node, cur_dist = dnode.node, dnode.dist
        for edge, node in cur_node.link_list:
            if node.nodeid in node_set:
                continue
            node_set.add(node.nodeid)
            next_dnode = DistNode(node, cur_dist + edge.edge_length)
            node.prev_node = cur_node
            q.put(next_dnode)
            if edge.edge_index == cur_edge.edge_index:
                edge_found = True

    trace = [cur_point]
    n0, n1 = cur_edge.node0, cur_edge.node1
    if n0.prev_node == n1:  # n1 is nearer from last point
        cur_node = n1
    else:
        cur_node = n0
    while cur_node != last_edge.node0 and cur_node != last_edge.node1:
        trace.append(cur_node.point)
        cur_node = cur_node.prev_node

    trace.append(cur_node.point)
    trace.append(spoint)
    return trace, get_trace_dist(trace)


def get_trace(last_edge, edge, last_point, point):
    """
    use prev_node to generate the path reversely
    :param last_edge:  last matched edge
    :param edge:  current matched edge
    :param last_point:  last position point
    :param point:  current matched(projected) point
    :return:
    """
    spoint, _, _ = point_project_edge(last_point, last_edge)
    if last_edge == edge:
        return [spoint, point]

    trace = [point]
    n0, n1 = edge.node0, edge.node1
    if n0.prev_node == n1:      # n1 is nearer from last point
        cur_node = n1
    else:
        cur_node = n0
    while cur_node != last_edge.node0 and cur_node != last_edge.node1:
        trace.append(cur_node.point)
        cur_node = cur_node.prev_node

    trace.append(cur_node.point)
    trace.append(spoint)
    return trace


def PNT_MATCH(traj_order):
    """
    using point match with topology, 
    :param traj_order: list of Taxi_Data 
    :return: 
    """
    kdt, X = make_kdtree()
    first_point = True
    last_data, last_point, last_edge = None, None, None
    last_state = 0      # 判断双向道路当前是正向或者反向
    total_dist = 0.0    # 计算路程
    last_time = None
    cnt = 0
    traj_mod = []
    for data in traj_order:
        if first_point:
            # 第一个点
            candidate_edges = get_candidate_first(data, kdt, X)
            # Taxi_Data .px .py .stime .speed
            first_point = False
            mod_point, last_edge, _ = get_mod_point(data, candidate_edges, last_point, cnt)
            state = 'c'
            traj_mod.append(mod_point)
            last_point = mod_point
            last_time = data.stime
        else:
            # 随后的点
            # 首先判断两个点是否离得足够远
            T = 15
            cur_point = [data.px, data.py]
            interval = calc_dist(cur_point, last_point)
            interval_time = (data.stime - last_time).total_seconds()
            # print cnt, interval
            if interval < T:
                last_time = data.stime
                continue
            candidate_edges = get_candidate_later(data, last_data, last_point, last_edge, last_state, interval_time, cnt)
            # if cnt == 23:
            #     print data.stime, last_time, interval_time
            #     draw_edge_list(candidate_edges)

            if len(candidate_edges) == 0:
                # no match, restart
                candidate_edges = get_candidate_first(data, kdt, X)
                mod_point, cur_edge, _ = get_mod_point(data, candidate_edges, None, cnt)
                state = 'c'
            else:
                mod_point, cur_edge, _ = get_mod_point(data, candidate_edges, last_point, cnt)
                state = 'r'

            offset_dist = calc_dist(mod_point, cur_point)
            if offset_dist > 60:
                # 判断是否出现太远的情况
                candidate_edges = get_candidate_first(data, cnt, X)
                # draw_edge_list(candidate_edges)
                mod_point, cur_edge, _ = get_mod_point(data, candidate_edges, None, cnt)
                state = 'm'

            if state == 'r':
                trace = get_trace(last_edge, cur_edge, last_point, mod_point)
                draw_seg(trace, 'b')
                dist = get_trace_dist(trace)
            else:
                dist = calc_dist(cur_point, last_point)
            total_dist += dist

            traj_mod.append(mod_point)
            last_point, last_edge = cur_point, cur_edge

        plt.text(data.px, data.py, '{0}'.format(cnt))
        plt.text(mod_point[0], mod_point[1], '{0}'.format(cnt), color=state)

        cnt += 1
        last_time, last_data = data.stime, data
        # print cnt, data.px, data.py, mod_point[0], mod_point[1]

    return traj_mod, total_dist


def DYN_MATCH(traj_order):
    """
    T.B.M.
    using point match with dynamic programming, 
    :param traj_order: list of Taxi_Data 
    :return: 
    """
    kdt, X = make_kdtree()
    first_point = True
    last_data, last_point = None, None
    last_state = 0  # 判断双向道路当前是正向或者反向
    total_dist = 0.0  # 计算路程
    last_time = None
    cnt = 0
    match_results = []     # MatchResult

    for data in traj_order:
        bt = clock()
        if first_point:
            # 第一个点
            candidate_edges = get_candidate_first(data, kdt, X)
            # Taxi_Data .px .py .stime .speed
            first_point = False
            mod_point, last_edge, dist = get_mod_point(data, candidate_edges, last_point, cnt)
            mr = MatchResult([data.px, data.py])
            mr.add_match(last_edge.edge_index, mod_point, [], dist, dist)
            mr.set_first(True)      # 首次匹配
            match_results.append(mr)
            last_point = mod_point
        else:
            # 随后的点
            # 首先判断两个点是否离得足够远
            T = 10
            cur_point = [data.px, data.py]
            interval = calc_dist(cur_point, last_point)
            # print cnt, interval
            if interval < T:
                last_time = data.stime
                continue
            cur_point = [data.px, data.py]
            interval_time = (data.stime - last_time).total_seconds()

            last_mr = match_results[cnt - 1]
            has_result = False
            ret = {}
            # 对于上一次匹配的每一条记录进行运算

            mr = MatchResult(cur_point)
            for i, mp in enumerate(last_mr.match_point_list):
                last_index = mp.edge_index
                last_edge = map_edge[last_index]
                candidate_edges = get_candidate_later(data, last_data, last_point, last_edge,
                                                      last_state, interval_time, cnt)
                # if cnt == 9:
                #     draw_edge_list(candidate_edges)
                if len(candidate_edges) > 0:
                    # 正常情形
                    match_list = get_mod_points(data, candidate_edges, last_point, last_edge, cnt)
                    for mtc in match_list:
                        mp, edge, dist, score = mtc[0:4]
                        ei = edge.edge_index
                        try:
                            ret[ei][1].append(last_index)
                        except KeyError:
                            ret[ei] = [mp, [last_index], dist, score]
            for ei, v in ret.iteritems():
                has_result = True
                mr.add_match(ei, ret[ei][0], ret[ei][1], ret[ei][2], ret[ei][3])

            if has_result is False:             # 无匹配结果
                candidate_edges = get_candidate_first(data, kdt, X)
                # Taxi_Data .px .py .stime .speed
                first_point = False
                mod_point, last_edge, dist = get_mod_point(data, candidate_edges, last_point, cnt)
                mr.add_match(last_edge.edge_index, mod_point, [], dist, dist)

            mr.set_first(not has_result)
            match_results.append(mr)
            # print len(mr.match_list)
            last_point = cur_point

        plt.text(data.px, data.py, '{0}'.format(cnt))
        str_time = data.stime.strftime('%M:%S')
        # plt.text(data.px, data.py, '{0},{1},{2}'.format(cnt, str_time, data.speed))

        # print cnt, clock() - bt
        cnt += 1
        last_time = data.stime
        last_data = data

    return match_results, total_dist


def draw_trace(trace):
    x, y = [], []
    for data in trace:
        x.append(data.px)
        y.append(data.py)
    minx, maxx, miny, maxy = min(x), max(x), min(y), max(y)
    plt.xlim(minx, maxx)
    plt.ylim(miny, maxy)
    plt.plot(x, y, 'k--', marker='+')


def matching_draw(trace):
    # read_xml('hz.xml')
    fig = plt.figure(figsize=(16, 8))
    ax = fig.add_subplot(111)
    draw_map()
    draw_trace(trace)
    #
    bt = clock()
    traj_mod, dist = DYN_MATCH(trace)
    print 'matching_draw', clock() - bt
    calc_best_path(traj_mod)
    draw_mod_results(traj_mod)
    plt.show()
    return dist


def matching(trace):
    # read_xml('hz.xml')
    fig = plt.figure(figsize=(16, 8))
    ax = fig.add_subplot(111)
    draw_map()
    draw_trace(trace)
    bt = clock()
    traj_mod, dist = PNT_MATCH(trace)
    print clock() - bt
    plt.show()
    return dist

