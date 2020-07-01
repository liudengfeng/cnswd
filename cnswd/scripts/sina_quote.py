import asyncio
import re
import time
import warnings
from multiprocessing import Pool

import aiohttp
import logbook
import numpy as np
import pandas as pd

from ..mongodb import get_db
from ..scripts.trading_calendar import is_trading_day
from ..setting.constants import MARKET_START, MAX_WORKER, QUOTE_COLS
from ..utils import batch_loop, data_root, make_logger
from ..utils.db_utils import to_dict

db_name = 'quotes'
logger = make_logger('实时报价')
warnings.filterwarnings('ignore')
QUOTE_PATTERN = re.compile('"(.*)"')
CODE_PATTERN = re.compile(r'hq_str_s[zh](\d{6})')


def _convert_to_numeric(s, exclude=()):
    if pd.api.types.is_string_dtype(s):
        if exclude:
            if s.name not in exclude:
                return pd.to_numeric(s, errors='coerce')
    return s


def _to_dataframe(content):
    """解析网页数据，返回DataFrame对象"""
    res = [x.split(',') for x in re.findall(QUOTE_PATTERN, content)]
    codes = [x for x in re.findall(CODE_PATTERN, content)]
    df = pd.DataFrame(res).iloc[:, :32]
    df.columns = QUOTE_COLS[1:]
    df.insert(0, '股票代码', codes)
    df.dropna(inplace=True)
    return df


def _add_prefix(stock_code):
    pre = stock_code[0]
    if pre == '6':
        return 'sh{}'.format(stock_code)
    else:
        return 'sz{}'.format(stock_code)


async def fetch(codes):
    url_fmt = 'http://hq.sinajs.cn/list={}'
    url = url_fmt.format(','.join(map(_add_prefix, codes)))
    async with aiohttp.request('GET', url) as r:
        data = await r.text()
    return data


async def to_dataframe(codes):
    """解析网页数据，返回DataFrame对象"""
    content = await fetch(codes)
    df = _to_dataframe(content)
    df = df.apply(_convert_to_numeric, exclude=('股票代码', '股票简称', '日期', '时间'))
    df = df[df.成交额 > 0]
    if len(df) > 0:
        df['时间'] = pd.to_datetime(df.日期 + ' ' + df.时间)
        del df['日期']
        return df
    return pd.DataFrame()


async def fetch_all(batch_num=800):
    """获取所有股票实时报价原始数据"""
    db = get_db()
    collection = db['股票列表']
    stock_codes = collection.find_one()['codes']
    b_codes = batch_loop(stock_codes, batch_num)
    tasks = [to_dataframe(codes) for codes in b_codes]
    dfs = await asyncio.gather(*tasks)
    return pd.concat(dfs)


def create_index(collection):
    collection.create_index([("批次", -1)], name='dt_index')


async def refresh():
    """刷新实时报价"""
    db = get_db(db_name)
    today = pd.Timestamp('today')
    name = today.strftime(r"%Y-%m-%d")
    collection = db[name]
    # if name not in db.list_collection_names(False):
    #     create_index(collection)
    if collection.estimated_document_count() == 0:
        create_index(collection)
    # 后台计划任务控制运行时间点。此处仅仅判断当天是否为交易日
    if not is_trading_day(today):
        logger.warning(f"{today} 非交易日")
        return
    df = await fetch_all()
    if len(df) > 0:
        df = df.loc[df['时间'] >= today.normalize(), :]
        # 增加 `批次`字段，以便于分析 批与批之间变化趋势
        df['批次'] = today.ceil('s')
        if len(df):
            collection.insert_many(to_dict(df))
            logger.info(f'Inserted {df.shape[0]} rows')


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
