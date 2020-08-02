"""

刷新指数日线数据

"""
import time
from multiprocessing import Pool

import pandas as pd
from retry.api import retry_call

from ..mongodb import get_db
from ..setting.constants import MAIN_INDEX, MARKET_START, MAX_WORKER
from ..utils import ensure_dtypes
from ..utils.db_utils import to_dict
from ..utils.log_utils import make_logger
from ..websource.wy import fetch_history, get_index_base

logger = make_logger('网易指数日线')
db_name = "wy_index_daily"
col_dtypes = {
    'd_cols': ['日期'],
    's_cols': ['股票代码', '名称'],
    'i_cols': ['成交量', '成交笔数'],
}


def find_last_date(collection):
    res = collection.find_one(projection={'日期': 1}, sort=[('日期', -1)])
    return res['日期'] if res else MARKET_START.tz_localize(None)


def create_index(collection):
    collection.create_index([("日期", -1)], unique=True, name='dt_index')


def _fix_data(df):
    code_col = '股票代码'
    # 去掉股票代码前缀 '
    df[code_col] = df[code_col].map(lambda x: x[1:])
    return df


def _one(code):
    db = get_db(db_name)
    collection = db[code]
    if collection.estimated_document_count() == 0:
        create_index(collection)
    start = find_last_date(collection) + pd.Timedelta(days=1)
    start = pd.Timestamp(start)
    start_str = start.strftime(r"%Y-%m-%d")
    df = retry_call(fetch_history,
                    fkwargs={
                        'code': code,
                        'start': start,
                        'is_index': True
                    },
                    exceptions=(ConnectionError, ValueError),
                    delay=0.3,
                    logger=logger,
                    tries=3)
    if df.empty:
        logger.info(f"指数代码 {code} 开始日期 {start_str} 数据为空")
        return
    df.reset_index(inplace=True)
    df = ensure_dtypes(df, **col_dtypes)
    fixed = _fix_data(df)
    fixed.drop(['股票代码'], axis=1, inplace=True)
    docs = to_dict(fixed)
    collection.insert_many(docs)
    logger.info(f"指数代码 {code} 开始日期 {start_str} 插入 {len(docs)} 行")


def refresh():
    t = time.time()
    # codes = MAIN_INDEX.keys()
    codes = get_index_base().to_dict()['name'].keys()
    for code in codes:
        try:
            _one(code)
        except Exception as e:
            print(f"{e!r}")
    logger.info(f"指数数量 {len(codes)}, 用时 {time.time() - t:.4f}秒")
