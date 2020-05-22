import os
from multiprocessing import Pool

import pandas as pd
from tenacity import retry, retry_if_exception_type, stop_after_attempt

from ..setting.constants import MAX_WORKER
from ..utils import data_root, make_logger
from ..websource.yahoo import (fetch_ebitda, fetch_free_cash_flow,
                               fetch_history, fetch_total_assets)

from ..websource.tencent import get_recent_trading_stocks


def file_path(code, item):
    return data_root(f"yahoo/{item}/{code}.pkl")


def get_start(code, item):
    fn_path = file_path(code, item)
    if os.path.exists(fn_path):
        # 最新修改时间
        st_mtime = pd.Timestamp(os.stat(fn_path).st_mtime, unit='s')
        # 如最新修改时间为当天，则返回`明天`
        if st_mtime.date() == pd.Timestamp.today().date():
            return pd.Timestamp.today().normalize() + pd.Timedelta(days=1)
        last_date = pd.read_pickle(fn_path)['date'].values[-1]
        return pd.Timestamp(last_date) + pd.Timedelta(days=1)
    return pd.Timestamp('1991-01-01')


@retry(retry=retry_if_exception_type(ValueError), stop=stop_after_attempt(3))
def _one_stock(code):
    today = pd.Timestamp.today().normalize()
    if today.weekday() == 6:
        today = today - pd.Timedelta(days=1)
    elif today.weekday() == 0:
        today = today - pd.Timedelta(days=2)
    for func in (fetch_ebitda, fetch_free_cash_flow, fetch_history,
                 fetch_total_assets):
        name = func.__name__.replace('fetch_', '')
        if name != 'history':
            for period_type in ('quarterly', 'annual'):
                item = f"{period_type}_{name}"
                logger = make_logger(item)
                start = get_start(code, item)
                if start > today:
                    info = f"{code} {start.strftime(r'%Y-%m-%d')} ~ {today.strftime(r'%Y-%m-%d')} 无数据"
                    logger.info(info)
                    continue
                web_data = func(code, period_type=period_type)
                fp = file_path(code, item)
                if not os.path.exists(fp):
                    web_data.to_pickle(fp)
                    rows = len(web_data)
                else:
                    old_data = pd.read_pickle(fp)
                    df = pd.concat([old_data, web_data],
                                   sort=False).drop_duplicates(subset='date')
                    df.to_pickle(fp)
                    rows = len(df)
                info = f"添加 {code} {start.strftime(r'%Y-%m-%d')} ~ {today.strftime(r'%Y-%m-%d')} {rows}行"
                logger.info(info)


def refresh_all():
    stock_codes = get_recent_trading_stocks()
    with Pool(MAX_WORKER) as pool:
        pool.map(_one_stock, stock_codes)


def read_data(code, item, period_type='quarterly'):
    """读取本地项目数据

    Arguments:
        code {str} -- 股票代码
        item {str} -- 项目名称

    Keyword Arguments:
        period_type {str} -- 周期类别 (default: {'quarterly'})
    """
    if item != 'history':
        item = f"{period_type}_{item}"
    fp = file_path(code, item)
    return pd.read_pickle(fp)
