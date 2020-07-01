import time
from multiprocessing import Pool

import pandas as pd

from ..mongodb import get_db
from ..scripts.trading_calendar import is_trading_day
from ..setting.constants import MAX_WORKER
from ..utils import make_logger
from ..utils.db_utils import to_dict
from ..websource.tencent import fetch_minutely_prices

db_name = 'minutely'
logger = make_logger('分时交易')


def create_index(collection):
    collection.create_index([("时间", -1)], name='dt_index')


def refresh():
    """刷新分钟交易数据"""
    today = pd.Timestamp('today')
    # 后台计划任务控制运行时间点。此处仅仅判断当天是否为交易日
    if not is_trading_day(today):
        return
    df = fetch_minutely_prices()
    if len(df) > 0:
        dt = pd.Timestamp.now().floor('min')
        df.reset_index(inplace=True)
        df['时间'] = dt
        df.rename(columns={'代码': '股票代码'}, inplace=True)
        df['股票代码'] = df['股票代码'].map(lambda x: x[2:])
        db = get_db(db_name)
        name = dt.strftime(r"%Y-%m-%d")
        collection = db[name]
        # 创建索引
        if collection.estimated_document_count() == 0:
            create_index(collection)
        collection.insert_many(to_dict(df))
        logger.info('添加{}行'.format(df.shape[0]))


def to_db(g):
    g_name, group = g
    name = g_name.strftime(r"%Y-%m-%d")
    print(f"Group {name}")
    db = get_db(db_name)
    collection = db[name]
    # 创建索引
    if collection.estimated_document_count() == 0:
        create_index(collection)
    collection.insert_many(to_dict(group))
