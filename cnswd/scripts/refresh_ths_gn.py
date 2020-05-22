"""
覆盖式更新

同花顺网站禁止多进程提取数据
"""
import math
import os
import random
import time
import warnings

import pandas as pd

from ..store import ThsGnStore
from ..utils import (data_root, is_trading_time,
                     make_logger)
from ..websource.ths import THS

logger = make_logger('同花顺')


def _update_gn_list(api, urls):
    codes = [x[0][-7:-1] for x in urls]
    d = {x[0][-7:-1]: x[1] for x in urls}
    dfs = []
    status = {}
    # 测试用
    # codes = ['300023']  # ,'301636', '300337']
    for gn in codes:
        for _ in range(3):
            if status.get(gn, False):
                break
            try:
                df = api.get_gn_detail(gn)
                df['概念'] = d[gn]
                dfs.append(df)
                status[gn] = True
                logger.info('提取 {} {}行'.format(d[gn], df.shape[0]))
            except Exception as e:
                status[gn] = False
                logger.error(f'{e!r}')
        time.sleep(0.1)
    data = pd.concat(dfs, sort=True)
    data.drop(columns=['index'], inplace=True, errors='ignore')
    failed = [k for k, v in status.items() if not v]
    if len(failed):
        print('失败：', ' '.join(failed))
    return data


def get_gn_list(api):
    """
    更新股票概念列表

    非交易时段更新有效
    """
    if is_trading_time():
        warnings.warn('建议非交易时段更新股票概念。交易时段内涨跌幅经常变动，容易产生重复值！！！')
        return None
    urls = api.gn_urls
    return _update_gn_list(api, urls)


def refresh():
    with THS() as api:
        t1 = api.gn_times  # TODO:8/28 行数9 ValueError: No tables found
        t2 = get_gn_list(api)
    t1.drop(columns=['index'], inplace=True, errors='ignore')
    ThsGnStore.put(t1, 'time')
    ThsGnStore.put(t2, 'df')
