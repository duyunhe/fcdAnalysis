# -*- coding: utf-8 -*-
# @Time    : 2018/10/30 9:44
# @Author  : yhdu@tongwoo.cn
# @简介    : 
# @File    : map_matching.py


# -*- coding: utf-8 -*-
# @Time    : 2018/8/13 10:03
# @Author  :
# @简介    : 地图匹配模块
# @File    : map_matching.py

from map_struct import MapEdge, MapNode, MapRoad
from geo_multi import point2segment, point_project, calc_included_angle, calc_dist
import matplotlib.pyplot as plt
from DBConn import oracle_util
from sklearn.neighbors import KDTree
import numpy as np

import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class MapInfo:
    """
    保存地图信息
    """
    def __new__(cls):
        # 单例。每一次实例化的时候，只会返回同一个instance对象
        if not hasattr(cls, 'instance'):
            cls.instance = super(MapInfo, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.map_node = {}  # 地图节点MapNode
        self.map_edge = []  # 地图边，每一线段都是一条MapEdge，MapEdge两端是MapNode
        self.map_road = {}  # 记录道路信息
        self.kdt, self.X = None, None

    def load_map(self):
        conn = oracle_util.get_connection()

        sql = "select * from tb_road_state where direction = 0"
        # 单行线不予录入
        cursor = conn.cursor()
        cursor.execute(sql)
        for item in cursor:
            road_name, direction, level = item[1:4]
            r = MapRoad(road_name, direction, level)
            rid = item[0]
            self.map_road[rid] = r

        map_temp_node = {}  # 维护地图节点
        road_point = {}
        sql = "select rp.* from tb_road_point_xy rp, tb_road_state" \
              " rs where rp.rid = rs.rid and rs.direction = 0 order by rp.rid, rp.seq "
        cursor.execute(sql)
        for item in cursor:
            rid = item[0]
            lng, lat = map(float, item[2:4])
            try:
                road_point[rid].append([lng, lat])
            except KeyError:
                road_point[rid] = [[lng, lat]]

        # 维护节点dict
        # 每一个点都单独列入dict，边用两点表示，道路用若干点表示
        for rid, bllist in road_point.iteritems():
            r = self.map_road[rid]
            last_nodeid = -1
            lastx, lasty = None, None
            for i, bl in enumerate(bllist):
                x, y = bl[0:2]
                str_bl = "{0},{1}".format(x, y)
                # x, y = bl2xy(lat, lng)
                try:
                    nodeid = map_temp_node[str_bl]
                    nd = self.map_node[nodeid]
                except KeyError:
                    nodeid = len(map_temp_node)
                    map_temp_node[str_bl] = nodeid
                    nd = MapNode([x, y], nodeid)
                    self.map_node[nodeid] = nd
                if i > 0:
                    edge_length = calc_dist([x, y], [lastx, lasty])
                    edge = MapEdge(self.map_node[last_nodeid], self.map_node[nodeid], True,
                                   len(self.map_edge), edge_length, rid)
                    self.map_edge.append(edge)
                r.add_node(nd)
                last_nodeid, lastx, lasty = nodeid, x, y

        conn.close()

    def store_link(self):
        for edge in self.map_edge:
            n0, n1 = edge.node0, edge.node1
            if edge.oneway is True:
                n0.add_link(edge, n1)
                n1.add_rlink(edge, n0)
            else:
                n0.add_link(edge, n1)
                n1.add_link(edge, n0)
                n0.add_rlink(edge, n1)
                n1.add_rlink(edge, n0)

    def init_map(self):
        self.load_map()
        self.store_link()

    def get_node(self):
        return self.map_node

    def get_edge(self):
        return self.map_edge


class MapMatching(object):
    """
    封装地图匹配类
    在init时调用MapInfo.init_map，读取地图及初始化数据结构
    单点匹配时调用PNT_MATCH，传入当前GPS点的位置和车辆标识，得到匹配点和匹配边
    """

    def __init__(self):
        print "map matching init..."
        self.mi = MapInfo()
        self.mi.init_map()
        self.map_node = self.mi.get_node()
        self.map_edge = self.mi.get_edge()
        self.nodeid_list = []
        self.kdt, self.X = self.make_kdtree()
        self.last_edge_list, self.mod_list = {}, {}     # 记录每辆车之前的点
        self.state_list = {}                            # 记录上一个状态
        print "map matching ready"

    def make_kdtree(self):
        nd_list = []
        for key, item in self.mi.map_node.items():
            self.nodeid_list.append(key)
            nd_list.append(item.point)
        X = np.array(nd_list)
        return KDTree(X, leaf_size=2, metric="euclidean"), X

    def plot_map(self, road_speed):
        """
        根据路况绘制地图
        :param road_speed: {rid:speed_list}
        speed_list :[spd, sample_number, tti]
        :return: 
        """
        for rid, road in self.mi.map_road.iteritems():
            node_list = road.node_list
            x, y = [], []
            for node in node_list:
                x.append(node.point[0])
                y.append(node.point[1])
            try:
                speed, _, tti = road_speed[rid]
                if tti > 8:
                    c = 'm'
                elif 6 < tti <= 8:
                    c = 'red'
                elif 4 < tti <= 6:
                    c = 'darkorange'
                elif 2 < tti <= 4:
                    c = 'green'
                else:
                    c = 'green'
            except KeyError:
                c = 'k'
            plt.plot(x, y, c=c, alpha=0.3, linewidth=2)

            if c == 'm' or c == 'red':
                plt.text((x[0] + x[-1]) / 2, (y[0] + y[-1]) / 2, "{0}".format(rid))
            # plt.text(x[0], y[0], "{0},{1}".format(rid, speed))

        # for e in self.mi.map_edge:
        #     if e.edge_index == 370 or e.edge_index == 371:
        #         draw_edge(e, 'g')
        plt.show()

    def get_candidate_first(self, taxi_data, kdt, X):
        """
        get candidate edges from road network which fit point 
        :param taxi_data: Taxi_Data  .px, .py, .speed, .stime
        :param kdt: kd-tree
        :return: edge candidate list  list[edge0, edge1, edge...]
        """
        dist, ind = kdt.query([[taxi_data.px, taxi_data.py]], k=15)

        pts = []
        seg_set = set()
        # fetch nearest map nodes in network around point, then check their linked edges
        for i in ind[0]:
            pts.append([X[i][0], X[i][1]])
            node_id = self.nodeid_list[i]
            edge_list = self.map_node[node_id].link_list
            for e, nd in edge_list:
                seg_set.add(e.edge_index)
            # here, need reverse link,
            # for its first node can be far far away, then this edge will not be included
            edge_list = self.map_node[node_id].rlink_list
            for e, nd in edge_list:
                seg_set.add(e.edge_index)

        edge_can_list = []
        for i in seg_set:
            edge_can_list.append(self.map_edge[i])

        return edge_can_list

    def _get_mod_point_first(self, candidate, point):
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

    def _get_mod_point_later(self, candidate, point, last_point, cnt):
        """
        :param candidate: 
        :param point: current position point
        :param last_point: last position point
        :return: project_point, sel_edge, score
        """
        min_score, sel_edge, sel_angle = 1e10, None, 0
        min_dist = 1e10

        for edge in candidate:
            p0, p1 = edge.node0.point, edge.node1.point
            w0, w1 = 1.0, 10.0
            # 加权计算分数，考虑夹角的影响
            dist = point2segment(point, p0, p1)
            angle = calc_included_angle(last_point, point, p0, p1)
            if not edge.oneway and angle < 0:
                angle = -angle
            if angle < 0.3:
                continue
            score = w0 * dist + w1 * (1 - angle)
            # if cnt == 4:
            #     print edge.edge_index, dist, score, angle
            if score < min_score:
                min_score, sel_edge, min_dist, sel_angle = score, edge, dist, angle
        # print min_score, sel_edge.way_id, min_dist, sel_angle

        # if min_dist > 100:
        #     return None, None, 0
        if sel_edge is None:
            return None, None, 0
        project_point, _, state = point_project(point, sel_edge.node0.point, sel_edge.node1.point)
        if state == 1:
            # 点落在线段末端外
            project_point = sel_edge.node1.point
        elif state == -1:
            project_point = sel_edge.node0.point
        return project_point, sel_edge, min_score

    def get_mod_point(self, taxi_data, last_data, candidate, last_point, cnt=-1):
        """
        get best fit point matched with candidate edges
        :param taxi_data: Taxi_Data
        :param last_data: last Taxi_Data
        :param candidate: list[edge0, edge1, edge...]
        :param last_point: last position point ([px, py])
        :param cnt: for debug, int
        :return: matched point, matched edge, minimum distance from point to matched edge
        """
        point = [taxi_data.px, taxi_data.py]
        if last_point is None:
            # 第一个点
            try:
                last_point = [last_data.px, last_data.py]
            except AttributeError:
                last_point = None
        if last_point is None:
            return self._get_mod_point_first(candidate, point)
        else:
            return self._get_mod_point_later(candidate, point, last_point, cnt)

    def PNT_MATCH(self, data, last_data, cnt=-1):
        """
        点到路段匹配，仅考虑前一个点
        :param data: 当前的TaxiData，见本模块
        :param last_data: 上一TaxiData数据
        :param cnt:  for test, int
        :return: 本次匹配到的点 cur_point (Point) 本次匹配到的边 cur_edge (Edge)
        """
        # 用分块方法做速度更快
        # 实际上kdtree效果也不错，所以就用kdtree求最近节点knn
        candidate_edges = self.get_candidate_first(data, self.kdt, self.X)
        # if cnt == 4:
        #     draw_edge_list(candidate_edges)
        if last_data is None:
            last_point = None
        else:
            last_point = [last_data.px, last_data.py]
        cur_point, cur_edge, score = self.get_mod_point(data, last_data,
                                                        candidate_edges, last_point, cnt)
        # 注意：得分太高（太远、匹配不上）会过滤
        if score > 100:
            cur_point, cur_edge = None, None
        return cur_point, cur_edge


