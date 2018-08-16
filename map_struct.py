# coding=utf-8


class DistNode(object):
    def __init__(self, node, dist):
        self.node = node
        self.dist = dist

    def __lt__(self, other):
        return self.dist < other.dist


class MapRoad(object):
    def __init__(self, road_name, direction, level):
        self.road_name, self.direction, self.lev = road_name, direction, level
        self.node_list = []         # 代表道路中线段的序列

    def add_node(self, map_node):
        self.node_list.append(map_node)


class MapNode(object):
    """
    点表示
    point([px,py]), nodeid, link_list, rlink_list, dist_dict
    在全局维护dict, key=nodeid, value=MapNode
    """
    def __init__(self, point, nodeid):
        self.point, self.nodeid = point, nodeid
        self.link_list = []         # 连接到其他点的列表, [[edge0, node0], [edge1, node1]....]
        self.rlink_list = []
        self.prev_node = None       # bfs时寻找路径, MapNode
        self.prev_edge = None

    def add_link(self, edge, node):
        self.link_list.append([edge, node])

    def add_rlink(self, edge, node):
        self.rlink_list.append([edge, node])


class MapEdge(object):
    """
    线段表示
    node0(MapNode), node1,
    oneway(true or false), edge_index, edge_length
    维护list[MapEdge]
    """
    def __init__(self, node0, node1, oneway, edge_index, edge_length, way_id):
        self.node0, self.node1 = node0, node1
        self.oneway = oneway
        self.edge_index = edge_index
        self.edge_length = edge_length
        self.way_id = way_id


class MatchResult(object):
    """
    匹配结果
    current point
    match_list: [MatchList, ...]
    """
    class MatchPoint(object):
        """
        edge_index, match_point, [last_index1, last_index2...], dist, score
        """
        def __init__(self, edge_index, mod_point, last_index_list, dist, score):
            self.edge_index, self.last_index_list = edge_index, last_index_list
            self.mod_point = mod_point
            self.dist, self.score = dist, score

    def __init__(self, point):
        self.point, self.first = point, True
        self.match_point_list = []
        self.sel = -1

    def add_match(self, edge_index, mod_point, index_list, dist, score):
        mp = self.MatchPoint(edge_index, mod_point, index_list, dist, score)
        self.match_point_list.append(mp)

    def set_sel(self, sel):
        self.sel = sel

    def set_first(self, first):
        self.first = first
