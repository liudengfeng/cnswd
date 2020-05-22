import random
import time
import warnings

import pandas as pd

from ..cninfo import ThematicStatistics
from ..cninfo.utils import get_field_type
from ..setting.config import TS_CONFIG
from ..store import MarginStore
from ..utils import ensure_dtypes

warnings.filterwarnings('ignore')


def _remove_data_of(d):
    """由于沪深二地数据不同步导致数据可能不完整。首先删除最后一日数据"""
    store = MarginStore.get_store()
    dt_col = MarginStore.INDEX_COL[0]
    where = f"{dt_col} = {pd.Timestamp(d)!r}"
    store.remove('df', where)


def get_dates():
    """循环列表"""
    min_start = TS_CONFIG['8.2']['date_field'][1]
    min_start = pd.Timestamp(min_start)
    start = MarginStore.get_attr('max_dt', min_start)
    end = pd.Timestamp('now')
    # 以时点判断结束日期，昨日或前日
    if end.hour >= 9:
        end = end - pd.Timedelta(days=1)
    else:
        end = end - pd.Timedelta(days=2)
    dates = pd.date_range(start, end, freq='B', normalize=True)
    dates = [d.strftime(r'%Y-%m-%d') for d in dates]
    return dates


def refresh():
    """刷新"""
    dfs = []
    dates = get_dates()
    if len(dates) == 0:
        return
    with ThematicStatistics() as api:
        # 自最后日期起至昨日
        for d in dates:
            web_data = api.get_data('8.2', d, d)
            if not web_data.empty:
                # 需要限定日期，此时为字符串
                web_data = web_data[web_data['交易日期'] == d]
                dfs.append(web_data)
    if len(dfs) == 0:
        return
    _remove_data_of(dates[0])
    df = pd.concat(dfs)
    col_dtypes = get_field_type('ts', '8.2')
    df = ensure_dtypes(df, **col_dtypes)
    max_dt = df['交易日期'].max()
    MarginStore.append(df)
    MarginStore.set_attr('max_dt', max_dt)
    MarginStore.create_table_index(None)
    print(f'添加{len(df)}行')
    MarginStore.update_record(None)
