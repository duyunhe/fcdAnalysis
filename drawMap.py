# -*- coding: utf-8 -*-
# @Time    : 2018/8/14 11:00
# @Author  : 
# @简介    : 
# @File    : drawMap.py

import matplotlib.pyplot as plt


def edge2xy(e):
    x0, y0 = e.node0.point[0:2]
    x1, y1 = e.node1.point[0:2]
    return x0, y0, x1, y1


def draw_edge(e, c):
    x0, y0, x1, y1 = edge2xy(e)
    x, y = [x0, x1], [y0, y1]
    plt.plot(x, y, c, linewidth=2)
    plt.text((x[0] + x[-1]) / 2, (y[0] + y[-1]) / 2, '{0}'.format(e.edge_index))


def draw_edge_list(edge_list):
    for edge in edge_list:
        if edge.oneway is True:
            draw_edge(edge, 'gold')
        else:
            draw_edge(edge, 'brown')
