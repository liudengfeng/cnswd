import pandas as pd
import click
from ..setting.constants import MARKET_START
from ..store import WyStockDailyStore
from ..websource.tencent import get_recent_trading_stocks
from ..websource.wy import fetch_history
from ..utils import ensure_dtypes


col_dtypes = {
    'd_cols': ['日期'],
    's_cols': ['股票代码', '名称'],
    'i_cols': ['成交量', '成交笔数'],
}


def _fix_data(df):
    code_col = '股票代码'
    # 去掉股票代码前缀 '
    df[code_col] = df[code_col].map(lambda x: x[1:])
    return df


def _one(code):
    try:
        record = WyStockDailyStore.get()
        old_max_dt = record.at[code, 'max_dt']
    except KeyError:
        old_max_dt = MARKET_START.tz_localize(None)
    start = old_max_dt + pd.Timedelta(days=1)
    df = fetch_history(code, start)
    df = ensure_dtypes(df, **col_dtypes)
    fixed = _fix_data(df)
    WyStockDailyStore.append(fixed)


def refresh():
    codes = get_recent_trading_stocks()
    with click.progressbar(codes,
                           length=len(codes),
                           show_eta=True,
                           item_show_func=lambda x: f"代码：{x}" if x is not None else '完成',
                           label="网易股票") as pbar:
        for code in pbar:
            try:
                _one(code)
            except Exception as e:
                print(f"{code} \n {e!r}")
    WyStockDailyStore.update_record(None)
    WyStockDailyStore.create_table_index(None)
