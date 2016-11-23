# -*- coding: utf-8 -*-
"""
Created on Wed Nov 23 10:38:30 2016

@author: ZFang
"""

import asyncio, logging
import aiomysql

def log(sql, arg=()):
    logging.info('SQL: %s' % sql)


@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = yield from aiomysql.create_pool(
            host=kw.get('host','localhost'),
            port=kw.get('port',3306),
            user=kw['user'],
            password=kw['passowrd'],
            db=kw['db'],
            charset=kw.get('charset','utf8'),
            autocommit=kw.get('autocommit',True),
            maxsize=kw.get('maxsize',10),
            minsize=kw.get('minsize',1),
            loop=loop
)

@asyncio.coroutine
def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?','%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs