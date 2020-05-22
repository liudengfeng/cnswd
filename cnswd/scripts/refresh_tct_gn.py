"""
腾讯概念股票列表(覆盖式更新)
"""
import pandas as pd

from ..store import TctGnStore
from ..utils import data_root, make_logger
from ..websource.tencent import fetch_concept_stocks


logger = make_logger('腾讯概念')


def refresh():
    """采用覆盖式更新腾讯股票概念列表"""
    df = fetch_concept_stocks()
    df.rename(columns={'item_id': '概念id',
                       'item_name': '概念简称', 'code': '股票代码'}, inplace=True)
    TctGnStore.put(df, 'df')
    logger.notice(f"写入{len(df)}行")
