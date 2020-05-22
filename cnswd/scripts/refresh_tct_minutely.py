import logbook
import pandas as pd
from ..store import TctMinutelyStore
from ..websource.tencent import fetch_minutely_prices
from .trading_calendar import is_trading_day


logger = logbook.Logger('分钟交易数据')


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
        TctMinutelyStore.append(df)
        logger.info('添加{}行'.format(df.shape[0]))
