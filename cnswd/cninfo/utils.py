import pandas as pd
from collections import OrderedDict
from os import path
import re

NUM_PAT = re.compile(r'\d{1,}')
here = path.abspath(path.dirname(__file__))


def get_field_map(item, level, to_dict=True):
    """获取字段信息
    
    Arguments:
        item {str} -- 项目名称 db, ts
        level {str} -- 项目层级
    
    Keyword Arguments:
        to_dict {bool} -- 是否以字典形式输出 (default: {True})
    
    Returns:
        {dict or DataFrame} -- 项目字段信息
    """
    fp = path.join(here, 'api_doc', item, f"{level}.csv")
    df = pd.read_csv(fp, '\t')
    df.columns = df.columns.str.strip()
    if to_dict:
        return OrderedDict(
            {row['英文名称']: row['中文名称']
             for _, row in df.iterrows()})
    else:
        return df


def get_field_type(item, level):
    """获取列类型"""
    fp = path.join(here, 'api_doc', item, f"{level}.csv")
    df = pd.read_csv(fp, '\t', dtype={'类型': str})
    df.columns = df.columns.str.strip()
    d_cols, s_cols, i_cols, f_cols = [], [], [], []
    for _, row in df.iterrows():
        type_ = row['类型'][:3].lower()
        if type_ in ('dat', ):
            d_cols.append(row['中文名称'])
        elif type_ in ('var', 'char'):
            s_cols.append(row['中文名称'])
        elif type_ in ('big', 'int'):
            i_cols.append(row['中文名称'])
        elif type_ in ('dec', 'num'):
            f_cols.append(row['中文名称'])
    return {
        'd_cols': d_cols,
        's_cols': s_cols,
        'i_cols': i_cols,
        'f_cols': f_cols,
    }


def get_min_itemsize(item, level):
    """获取字符类列最小长度"""
    assert level in ('1', )
    fp = path.join(here, 'api_doc', item, f"{level}.csv")
    df = pd.read_csv(fp, '\t', dtype={'类型': str})
    df.columns = df.columns.str.strip()    
    ret = {}
    for _, row in df.iterrows():
        name = row['中文名称']
        type_ = row['类型'][:3].lower()
        if name == '股票代码':
            ret[name] = 6
            continue
        if name == '股票简称':
            ret[name] = 20
            continue
        if type_ in ('var', 'char'):
            len_ = re.findall(NUM_PAT, row['类型'])[0]
            ret[name] = int(len_)
    return ret