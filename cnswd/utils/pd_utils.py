import numpy as np
import pandas as pd
from pandas.api.types import infer_dtype
from pprint import pprint
import math
from .tools import ensure_list


def _concat(dfs):
    try:
        # 务必维持原始列顺序
        return pd.concat(dfs, ignore_index=True)
    except Exception:
        return pd.DataFrame()


def parse_fields_to_dt(df, columns, dt_fmt='%Y-%m-%d'):
    """将dataframe对象的列解析为Date对象"""
    columns = ensure_list(columns)
    for c in columns:
        df[c] = df[c].apply(pd.to_datetime, errors='coerce', format=dt_fmt)
    return df


def round_price_to_penny(df, ndigits=2):
    """将表中数字类列的小数点调整到指定位数，其余列维持不变"""
    penny_part = df.select_dtypes(include=[np.number], exclude=[np.integer])
    remaining_cols = df.columns.difference(penny_part.columns)
    return pd.concat(
        [df[remaining_cols],
         penny_part.apply(round, ndigits=ndigits)], axis=1)


def safety_exists_pkl(fp):
    """判断文件是否存在（安全处理空表）"""
    # 如文件大小为0，则删除
    if not fp.name.endswith('.pkl'):
        raise NotImplementedError('不支持扩展名非pkl文件')
    if fp.exists():
        # 首先判断文件大小
        if fp.stat().st_size == 0:
            fp.unlink()
            return False
        # 为空视同不存在
        if pd.read_pickle(fp).empty:
            return False
        return True
    else:
        return False


def _to_int(x):
    try:
        return int(x)
    except ValueError:
        return 0


def ensure_dtypes(df, d_cols, s_cols, i_cols, f_cols=None):
    """转换列类型

    Arguments:
        df {DataFrame} -- 数据框对象
        d_cols {list}} -- 日期列列表
        s_cols {list} -- 字符串列列表
        i_cols {list} -- 整数列列表

    Keyword Arguments:
        f_cols {list} -- 数字列列表 (default: {None})

    Returns:
        DataFrame -- 修正数据类型后的数据框
    """
    if f_cols is None:
        f_cols = set(df.columns).difference(set(d_cols + s_cols + i_cols))
    for d_col in d_cols:
        if d_col in df.columns:
            df[d_col] = pd.to_datetime(df[d_col], errors='coerce')
    for s_col in s_cols:
        if s_col in df.columns:
            df[s_col] = df[s_col].astype(str)
    for i_col in i_cols:
        # 注意：当含有Na时，float 不能降级 integer
        # 分二步：第一步替换Nan为0；第二步转换数据类型
        if i_col in df.columns:
            df[i_col] = pd.to_numeric(df[i_col],
                                      downcast='integer',
                                      errors='coerce')
            df[i_col] = df[i_col].replace(np.nan, 0)
            df[i_col] = df[i_col].astype('int64', errors='ignore')
    for f_col in f_cols:
        if f_col in df.columns:
            df[f_col] = pd.to_numeric(df[f_col],
                                      downcast='float',
                                      errors='coerce').astype('float64',
                                                              errors='ignore')
    return df


def gen_min_itemsize(df, print_result=False):
    """生成min_itemsize辅助工具(观察字符型字段长度)"""
    res = {}
    for col in df.columns:
        if infer_dtype(df[col]) == 'string':
            l = df[col].str.len().max()
            res[col] = l
    if print_result:
        pprint(res)
    return res
