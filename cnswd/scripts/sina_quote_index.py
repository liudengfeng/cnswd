import asyncio
import re
import time
import warnings

import aiohttp
import logbook
import numpy as np
import pandas as pd
from toolz.itertoolz import partition_all
from ..mongodb import get_db
from ..scripts.trading_calendar import is_trading_day

from ..utils import data_root, make_logger
from ..utils.db_utils import to_dict

db_name = 'index_quotes'
logger = make_logger('股指实时报价')
warnings.filterwarnings('ignore')
QUOTE_PATTERN = re.compile('"(.*)"')

INDEX_QUOTE_COLS = [
    '指数简称', '最新价', '涨跌', '涨跌幅%', '成交量(万手)', '成交额(万元)'
]


def _rename(x):
    x = x.replace('Ａ', 'A')
    x = x.replace('Ｂ', 'B')
    return x


def _to_dataframe(content):
    """解析网页数据，返回DataFrame对象"""
    res = [x.split(',') for x in re.findall(QUOTE_PATTERN, content)]
    df = pd.DataFrame(res)
    df.columns = INDEX_QUOTE_COLS
    df['时间'] = pd.Timestamp.now().floor('T')
    df.dropna(inplace=True)
    return df


def _add_prefix(stock_code):
    pre = stock_code[0]
    if pre == '0':
        return 's_sh{}'.format(stock_code)
    else:
        return 's_sz{}'.format(stock_code)


async def fetch(codes):
    url_fmt = 'http://hq.sinajs.cn/list={}'
    url = url_fmt.format(','.join(map(_add_prefix, codes)))
    async with aiohttp.request('GET', url) as r:
        return await r.text()


async def to_dataframe(codes):
    """解析网页数据，返回DataFrame对象"""
    content = await fetch(codes)
    df = _to_dataframe(content)
    df.insert(0, '指数代码', codes)
    return df


async def fetch_all(batch_num=800):
    """获取所有股票实时报价原始数据"""
    db = get_db()
    collection = db['指数列表']
    stock_codes = collection.find_one()['codes']
    b_codes = partition_all(batch_num, stock_codes)
    tasks = [to_dataframe(codes) for codes in b_codes]
    dfs = await asyncio.gather(*tasks)
    return pd.concat(dfs)


def create_index(collection):
    collection.create_index([("时间", -1)], name='dt_index')


async def refresh():
    """刷新实时报价"""
    db = get_db(db_name)
    today = pd.Timestamp('today')
    name = today.strftime(r"%Y-%m-%d")
    collection = db[name]
    if collection.estimated_document_count() == 0:
        create_index(collection)
    # 后台计划任务控制运行时间点。此处仅仅判断当天是否为交易日
    if not is_trading_day(today):
        logger.warning(f"{today} 非交易日")
        return
    df = await fetch_all()
    if len(df) > 0:
        df = df.loc[df['时间'] >= today.normalize(), :]
        if len(df):
            df['指数简称'] = df['指数简称'].map(_rename)
            collection.insert_many(to_dict(df))
            logger.info(f'Inserted {df.shape[0]} rows')
