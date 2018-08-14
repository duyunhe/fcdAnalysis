# -*- coding: utf-8 -*-
# @Time    : 2018/3/27 9:39
# @Author  : 
# @简介    : 
# @File    : oracle_util.py

import cx_Oracle
import ConfigParser
from DBUtils.PooledDB import PooledDB


def get_connection():
    abs_file = __file__
    filename = abs_file[:abs_file.rfind("\\")] + '\config.ini'
    cf = ConfigParser.ConfigParser()
    fp = open(filename)
    cf.readfp(fp)

    host = cf.get('db', 'host')
    port = int(cf.get('db', 'port'))
    pswd = cf.get('db', 'pswd')
    sid = cf.get('db', 'sid')
    user = cf.get('db', 'user')
    sql_settings = {'oracle': {'user': user,
                               'password': pswd,
                               'dsn': '{0}:{1}/{2}'.format(host, port, sid)}}
    pool = PooledDB(creator=cx_Oracle,
                    mincached=20,
                    maxcached=200,
                    **sql_settings['oracle'])
    db_conn = pool.connection()
    return db_conn

