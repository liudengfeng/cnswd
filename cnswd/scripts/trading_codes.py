"""
所有股票代码
"""
import pandas as pd

from ..utils import make_logger
from ..mongodb import get_db
from ..websource.tencent import get_recent_trading_stocks
from ..websource.wy import fetch_history

logger = make_logger('股票列表')


def get_all_stock_codes():
    """所有股票代码

    Returns:
        list: 股票代码列表
    """
    db = get_db('wy_stock_daily')
    # 历史代码
    history = db.list_collection_names()
    # 最近一分钟交易的股票代码
    current = get_recent_trading_stocks()
    codes = set(history) | set(current)
    return list(sorted(codes))


def _refresh(collection):
    collection.drop()
    doc = {'codes': get_all_stock_codes(), 'update_time': pd.Timestamp.now()}
    collection.insert_one(doc)


def refresh():
    db = get_db('stockdb')
    collection = db['股票列表']
    try:
        update_time = collection.find_one()['update_time']
    except Exception:
        update_time = pd.Timestamp('1990-01-01')
    update_point = pd.Timestamp.now().normalize().replace(hour=9, minute=31)
    # 后台计划任务设定为 9:31 执行。以下简化判断逻辑，只要是应刷新而未刷新，就执行。
    if pd.Timestamp(update_time) < update_point:
        logger.info('刷新股票代码')
        _refresh(collection)


def read_all_stock_codes():
    """读取全部股票代码列表"""
    db = get_db('stockdb')
    collection = db['股票列表']
    codes = collection.find_one()['codes']
    if codes:
        return codes
    else:
        refresh()
        return read_all_stock_codes()
