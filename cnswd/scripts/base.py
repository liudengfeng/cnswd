from cnswd.websource.tencent import get_recent_trading_stocks
import akshare as ak
import pandas as pd
from toolz.dicttoolz import merge


def get_delist_stock_dates():
    """退市日期字典

    键：股票代码
    值：退市日期
    """
    sz_delist_df = ak.stock_info_sz_delist(indicator="终止上市公司")
    sh_delist_df = ak.stock_info_sh_delist(indicator="终止上市公司")
    res = {}
    for c, d in zip(sz_delist_df['证券代码'].values, sz_delist_df['终止上市日期'].values):
        if not pd.isnull(d):
            res[c] = pd.to_datetime(d).floor('D')
    for c, d in zip(sh_delist_df['COMPANY_CODE'].values, sh_delist_df['QIANYI_DATE'].values):
        if not pd.isnull(d):
            res[c] = pd.to_datetime(d).floor('D')
    return res


def get_stock_status():
    """股票状态词典

    键：股票代码
    值：退市日期。在市交易的股票代码其值为空
    """
    df = ak.stock_info_a_code_name()
    p1 = set(df['code'].to_list())
    p2 = set(get_recent_trading_stocks())
    codes = p1 | p2
    d1 = {code: None for code in codes}
    d2 = get_delist_stock_dates()
    # 注意，退市字典要放在次位置
    # 如有交叉键，则以次位置的值替代
    return merge(d1, d2)
