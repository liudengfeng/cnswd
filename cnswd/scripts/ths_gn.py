"""
覆盖式更新

同花顺网站对提取数据有限制

刷新非常耗时
"""
import re
import time
import random
import pandas as pd
import pymongo
from retry.api import retry_call
from selenium.common.exceptions import TimeoutException
from ..mongodb import get_db
from ..utils import is_trading_time, make_logger
from ..websource.ths import THS

FREQ = '7 days'  # 更新频率
coll_name = '同花顺概念'
logger = make_logger(coll_name)
GN_CODE_PAT = re.compile(r"(\d{6})")
db = get_db()
collection = db[coll_name]


def create_index(collection):
    collection.create_index([("日期", -1)], name='dt_index')


def update(old, new):
    if old:
        new['_id'] = old['_id']
        collection.find_one_and_replace({'_id': old['_id']}, new)
    else:
        collection.insert_one(new)


def _refresh(collection, api, infoes):
    for d in infoes:
        gn_code = d['概念编码']
        gn_name = d['概念名称']
        old = collection.find_one({'概念编码': gn_code})
        if old and (pd.Timestamp('now') - old['更新时间'] < pd.Timedelta(FREQ)):
            # 保留原数据
            continue
        else:
            add_info = retry_call(api.get_gn_detail, [gn_code, gn_name],
                                  tries=3,
                                  delay=0.3,
                                  logger=api.logger)
            d['股票列表'] = add_info['股票列表']
            d['概念定义'] = add_info['概念定义']
            d['更新时间'] = pd.Timestamp('now')
            update(old, d)
            api.logger.info(f"更新'{d['概念名称']}'")
            time.sleep(random.uniform(0.2, 0.5))


def refresh():
    if is_trading_time():
        logger.warning('在交易时段内获取股票概念分类数据会导致数据失真')
        return
    if collection.estimated_document_count() == 0:
        create_index(collection)
    with THS() as api:
        infoes = retry_call(api.get_gn_times,
                            tries=3,
                            delay=3,
                            exceptions=(TimeoutException, ),
                            logger=api.logger)

    def f():
        with THS() as api:
            retry_call(_refresh, [collection, api, infoes],
                       tries=3,
                       delay=1,
                       logger=api.logger)

    retry_call(f, tries=3, delay=3, exceptions=(TimeoutException, ))
