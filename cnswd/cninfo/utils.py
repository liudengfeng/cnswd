import pandas as pd
from functools import partial
import re

NUM_PAT = re.compile(r"([(]\d{1,}(,\d{1,})?[)])")


def _rename(old):
    if old == '证券代码':
        return '股票代码'
    if old == '证券简称':
        return '股票简称'
    new = old.replace('Ａ', 'A')
    new = new.replace('Ｂ', 'B')
    new = new.replace('Ｈ', 'H')
    return new


def _convert_func(field_type):
    type_ = NUM_PAT.sub('', field_type)
    type_ = type_.upper()
    if type_ in ('VARCHAR', 'CHAR'):
        return lambda x: x
    elif type_ in ('DATE', 'DATETIME'):
        return partial(pd.to_datetime, errors='coerce')
    elif type_ in ('BIGINT', 'INT'):
        return int
    elif type_ in ('DECIMAL', 'NUMERIC'):
        return lambda x: x
    raise ValueError(f"未定义类型'{type_}'")


def cleaned_data(data, field_maps):
    """清理后的数据

    Args:
        data (list): 字典列表
        field_maps (list): 字段元数据列表

    Returns:
        list: 整理后的数据
    """
    name_maps = {
        d['fieldName']: _rename(d['fieldChineseName'])
        for d in field_maps
    }
    convert_maps = {
        d['fieldName']: _convert_func(d['fieldType'])
        for d in field_maps
    }
    return [{
        name_maps.get(k, k): convert_maps.get(k, lambda x: x)(v)
        for k, v in row.items() if v
    } for row in data]
