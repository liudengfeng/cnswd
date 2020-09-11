import pandas as pd
from functools import partial
import re

NUM_PAT = re.compile(r"([(]\d{1,}(,\d{1,})?[)]?)")
N_PAT = re.compile(r"F\d{3}N")


def _rename(old, name=None, fieldName=None):
    if name == '财务指标行业排名' and fieldName == 'F003D':
        return '报告年度'
    if old == '证券代码':
        return '股票代码'
    if old == '证券简称':
        return '股票简称'
    new = old.replace('Ａ', 'A')
    new = new.replace('Ｂ', 'B')
    new = new.replace('Ｈ', 'H')
    return new


def _add_prefix(part_b):
    maps = sorted(list(part_b), key=lambda x: x['fieldName'])
    assert len(maps) % 3 == 0
    res = {}
    for i in range(len(maps) // 3):
        prefix = maps[i*3]['fieldChineseName']
        for j in range(3):
            loc = i*3 + j
            code = maps[loc]['fieldName']
            cname = maps[loc]['fieldChineseName']
            res[code] = f"{prefix}{cname}" if j != 0 else cname
    return res


def _get_field_name_maps(tname, field_maps):
    if tname != '财务指标行业排名':
        return {
            d['fieldName']: _rename(
                d['fieldChineseName'], tname, d['fieldName'])
            for d in field_maps
        }
    else:
        part_a = filter(lambda x: not N_PAT.match(x['fieldName']), field_maps)
        part_b = filter(lambda x: N_PAT.match(x['fieldName']), field_maps)
        res = {
            d['fieldName']: _rename(
                d['fieldChineseName'], tname, d['fieldName'])
            for d in part_a
        }
        res.update(_add_prefix(part_b))
        return res


def _convert_func(field_type, name=None, fieldName=None):
    # 将 `报告期` 统一为 `报告年度`
    if name == '财务指标行业排名' and fieldName == 'F003D':
        return partial(pd.to_datetime, errors='coerce')
    type_ = NUM_PAT.sub('', field_type)
    type_ = type_.upper()
    if type_ in ('VARCHAR', 'CHAR'):
        return lambda x: x
    elif type_ in ('DATE', 'DATETIME'):
        return partial(pd.to_datetime, errors='coerce')
    elif type_ in ('BIGINT', 'INT'):
        return int
    elif type_ in ('DECIMAL', 'NUMERIC', 'NUMBER'):
        return lambda x: x
    raise ValueError(f"未定义类型'{type_}'")


def cleaned_data(data, field_maps, name=None):
    """清理后的数据

    Args:
        data (list): 字典列表
        field_maps (list): 字段元数据列表

    Returns:
        list: 整理后的数据
    """
    name_maps = _get_field_name_maps(name, field_maps)
    convert_maps = {
        d['fieldName']: _convert_func(d['fieldType'], name, d['fieldName'])
        for d in field_maps
    }
    return [{
        name_maps.get(k, k): convert_maps.get(k, lambda x: x)(v)
        for k, v in row.items() if v
    } for row in data]
