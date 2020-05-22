"""
Valid comparison operators are:

=, ==, !=, >, >=, <, <=

Valid boolean expressions are combined with:

| : or

& : and

( and ) : for grouping

These rules are similar to how boolean expressions are used in pandas for indexing.

Note

= will be automatically expanded to the comparison operator ==

~ is the not operator, but can only be used in very limited circumstances

If a list/tuple of expressions is passed they will be combined via &

The following are valid expressions:

'index >= date'

"columns = ['A', 'D']"

"columns in ['A', 'D']"

"stock_code in ['000001', '000002']" 类似 isin

"stock_code = ['000001', '000002']"

'columns = A'

'columns == A'

"~(columns = ['A', 'B'])"

'index > df.index[3] & string = "bar"'

'(index > df.index[3] & index <= df.index[6]) | string = "bar"'

"ts >= Timestamp('2012-02-01')"

"major_axis>=20130101"
"""


from enum import Enum, unique
import pandas as pd
from cnswd.utils import ensure_dt_localize
import warnings


@unique
class Ops(Enum):
    eq = 1  # ==
    gte = 2  # >=
    lse = 3  # <=
    gt = 4  # >
    ls = 5  # <


def _to_op_symbol(e):
    if e == Ops.eq:
        return '=='
    elif e == Ops.gte:
        return '>='
    elif e == Ops.lse:
        return '<='
    elif e == Ops.gt:
        return '>'
    elif e == Ops.ls:
        return '<'
    raise ValueError(f'不支持比较操作符号{e}')


def force_freq_to_none(v):
    # 查询时间不得带tz,freq信息
    if isinstance(v, pd.Timestamp):
        if v.tz is not None or v.freq is not None:
            v = ensure_dt_localize(v)
            return pd.Timestamp(v.to_pydatetime()).tz_localize(None)
    return v


def query_stmt(*args, combined='and'):
    """生成查询表达式

    Notes:
    ------
        值`None`代表全部
    """
    assert combined in ('and', 'or')
    stmt = []  # combined定义连接关系
    for arg in args:
        assert len(arg) == 3, '子查询必须是（列名称、比较符、限定值）三元组'
        key, e, value = arg
        value = force_freq_to_none(value)
        if key is None or value is None or pd.isnull(value):
            continue
        stmt.append(f"{key} {_to_op_symbol(e)} {value!r}")
    split = ' & ' if combined == 'and' else ' | '
    return split.join(stmt) if stmt else None


def query(fp, stmt):
    if not fp.exists():
        raise FileNotFoundError(f"找不到文件：{fp}")
    try:
        store = pd.HDFStore(fp, mode='r')
        df = store.select('data', stmt, auto_close=True)
    except KeyError:
        # 当h5文件不存在data节点时触发
        warnings.warn(f'文件{fp}数据内容为空，请刷新项目数据。')
        df = pd.DataFrame()
    except Exception as e:
        warnings.warn(f"{e!r}")
        df = pd.DataFrame()
    else:
        store.close()
    return df
