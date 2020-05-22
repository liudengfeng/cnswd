"""
使用HdfStore类管理数据

pytables 3.6.2 版本即将完成线程安全

重要：
    不支持并行写入
    数据容易破坏。多数场景为：长时间处理文件时，其他写操作或中断导致数据截断，无法再打开。

    解决方案：
    1. 禁止并行实例化。
    2. 备份。至少可以恢复至上一次可打开的状态。

层级：
    1. /df 主数据
    2. /record 附加信息
    3. /refresh_time 上一次股票成功刷新完成时间
    4. 深证信分层数据
       /1/df
       /1/record
       /1/refresh_time
       ...
       /7.4.2/df
       /7.4.2/record
       /7.4.2/refresh_time
       

功能：
    1. 写入数据
    2. 查询数据

# HDFStore 
# put    覆盖
# append 添加
# remove 擦除

# 反复擦除数据后会急剧增加文件空间。压缩文件使用：
ptrepack --chunkshape=auto --propindexes --complevel=9 --complib=blosc:blosclz in.h5 out.h5
参数含义：
    --chunkshape=keep 默认keep，选auto
    --propindexes 传播原始表中的索引


备注：
    1. 数据以`dt`和`code`作为索引，`dt`为主索引，代码为次索引
    2. 如自定义属性`start_end`
       store.get_storer('df').attrs.start_end = obj 可自定义属性
    3. h5不支持并行写入，可并行读取
    4. ptrepack工具 重用先前删除的空间
"""
import os
import re
import time
from collections import defaultdict

import pandas as pd
from pandas.api.types import is_numeric_dtype

from ._exceptions import ForbidPparallel
from .query_utils import Ops, query_stmt
from .setting.config import DB_CONFIG
from .setting.constants import HDF_KWARGS, MARKET_START
from .utils import data_root

CODE_PAT = re.compile(r"\d{6}")
# DATE_PAT = re.compile(r"\d{4}-\d{2}-\d{2}")
# store使用状态记录文件路径
SR_FP = data_root('.store/record.pkl')


def mark_use(name):
    try:
        s = pd.read_pickle(SR_FP)
    except Exception:
        s = pd.Series()
    s[name] = True
    s.to_pickle(SR_FP)


def mark_end(name):
    try:
        s = pd.read_pickle(SR_FP)
    except Exception:
        s = pd.Series()
    s[name] = False
    s.to_pickle(SR_FP)


def is_using(name):
    try:
        return pd.read_pickle(SR_FP)[name]
    except Exception:
        return False

# 以下装饰器函数未使用
# 在@classmethod后
#   @classmethod
#   @on_start
#   def f(cls,*args):
#       pass
# 注意：第一个参数为`cls`


def on_start(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        cls = args[0]
        assert isinstance(cls, type)
        name = cls.FP_NAME
        if is_using(name):
            raise ValueError(f'由于同时操作容易导致数据损坏，禁止并行使用。数据库：{name}。')
        res = func(*args, **kwargs)
        mark_use(name)
        return res
    return wrapper


def on_finished(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        cls = args[0]
        assert isinstance(cls, type)
        name = cls.FP_NAME
        res = func(*args, **kwargs)
        mark_end(name)
        return res
    return wrapper


def ensure_dtype(df, col, dtype):
    if col not in df.columns:
        df[col] = 0
    else:
        cond = df[col].map(lambda x: is_numeric_dtype(x))
        df.loc[~cond, col] = 0
        df[col] = df[col].astype(dtype)
    return df


def _get_store(name, mode='a'):
    name = name.split('.')[0]
    fp = data_root(f"{name}.h5")
    return pd.HDFStore(fp, mode=mode)


def valid_codes(codes):
    res = [CODE_PAT.fullmatch(code) for code in codes]
    assert all(res), '股票代码列表中含非法代码'


def _codes_query_stmt(codes, code_col='股票代码'):
    if codes is None:
        return []
    elif len(codes) == 1:
        return query_stmt((code_col, Ops.eq, codes[0]))
    else:
        stmt = []
        valid_codes(codes)
        for code in codes:
            stmt.append((code_col, Ops.eq, code))
        return query_stmt(*stmt, combined='or')


def _period_query_stmt(start, end, dt_col):
    stmt = []
    if start:
        if isinstance(start, str):
            start = pd.Timestamp(start)
        assert isinstance(start, pd.Timestamp)
        assert dt_col, "当时间非空时，col_name不得为空"
        stmt.append((dt_col, Ops.gte, start))
    if end:
        if isinstance(end, str):
            end = pd.Timestamp(end)
        assert isinstance(end, pd.Timestamp)
        assert dt_col, "当时间非空时，col_name不得为空"
        stmt.append((dt_col, Ops.lse, end))
    # 期间查询如为同一天，则将end修正为次日零时前一秒
    if start and end and start == end:
        end = (end + pd.Timedelta(days=1)).normalize() - \
            pd.Timedelta(seconds=1)
        stmt.pop(-1)
        stmt.append((dt_col, Ops.lse, end))
    return query_stmt(*stmt)


def _query(fname, where, iterator, key='df'):
    store = _get_store(fname)
    # 不得使用with语句。
    res = store.select(key, where, iterator=iterator, auto_close=True)
    return res


__USAGE__ = """
    用法：
    >>> # 添加数据
    >>> C.append(df) # C.append(s)
    >>> # 设置属性
    >>> C.set_attr(name, value)
    >>> # 获取属性
    >>> C.get_attr(name, default)
    >>> # 覆盖式写入
    >>> C.put(df) # s or None
    >>> # 查询，默认名称为record
    >>> C.get()True
"""


class StoreBase(object):
    """数据基础类"""
    FP_NAME = ''
    INDEX_COL = (None, None)  # 注意顺序。时间第一，代码其次。

    @property
    def file_path(self):
        """数据库文件路径"""
        return data_root(f"{self.FP_NAME}.h5")

    @classmethod
    def get_store(cls):
        """数据存储对象"""
        return _get_store(cls.FP_NAME, 'a')

    @classmethod
    def get_nrows(cls, level=None):
        """数据表总行数"""
        key = f"{level}/df" if level else "df"
        with cls.get_store() as store:
            nrows = store.get_storer(key).nrows
        return nrows

    @classmethod
    def get_dt_col(cls, level=None):
        """时间列名称"""
        cls.verify_level(level)
        return cls.INDEX_COL[0]

    @classmethod
    def get_code_col(cls, level=None):
        """代码列名称"""
        cls.verify_level(level)
        return cls.INDEX_COL[1]

    @classmethod
    def get_attr(cls, name, default=None, level=None):
        """获取属性值"""
        key = f"{level}/df" if level else "df"
        with cls.get_store() as store:
            try:
                res = getattr(store.get_storer(key).attrs, name)
            except Exception:
                res = default
        return res

    @classmethod
    def set_attr(cls, name, value, level=None):
        """设置属性值"""
        key = f"{level}/df" if level else "df"
        with cls.get_store() as store:
            setattr(store.get_storer(key).attrs, name, value)

    @classmethod
    def get(cls, key='record'):
        """提取数据对象整体

        备注：主数据存放在`df`节点，用`query`方法查询
        """
        with cls.get_store() as store:
            return store.get(key)

    @classmethod
    def put(cls, data, key='record'):
        """覆盖式写入"""
        # record为Series或DataFrame或None
        with cls.get_store() as store:
            kw = HDF_KWARGS.copy()
            # 此处我们定义put为覆盖式写入
            kw.pop('append')
            store.put(key, data, **kw)

    @classmethod
    def create_table_index(cls, level):
        """创建表索引"""
        cls.verify_level(level)
        key = 'df' if level is None else f"{level}/df"
        with cls.get_store() as store:
            store.create_table_index(
                key, columns=True, optlevel=9, kind='full')

    @classmethod
    def _ensure_index(cls, df, level):
        """设置索引"""
        cls.verify_level(level)
        includes = [cls.get_dt_col(level), cls.get_code_col(level)]
        includes = [x for x in includes if x]
        if len(includes) == 0:
            return
        # 索引名称列表为`时间与代码`
        msg = f"数据框索引包含`{includes}`列"
        passed = []
        for col in includes:
            if col in df.index.names:
                passed.append(True)
        if not all(passed):
            raise ValueError(msg)

        # 就地更改方式设置索引
        df.reset_index(inplace=True)
        # 不保留原始序号
        df.drop(columns=['index'], inplace=True, errors='ignore')
        df.set_index(includes, inplace=True)
        return df

    @staticmethod
    def _fix_data(df):
        # 允许子类进行数据修复
        return df

    @classmethod
    def append(cls, df, kw={}, level=None):
        """添加数据到给定层

        Arguments:
            df {DataFrame} -- 数据对

        Keyword Arguments:
            kw {dict} -- 添加方式附加键值对参数 (default: {{}})
            level {str} -- 要添加数据的目标层 (default: {None})

        Raises:
            ValueError: 添加模式值异常
        """
        cls.verify_level(level)
        if not isinstance(df, pd.DataFrame):
            return
        if df.empty:
            return
        kw_ = HDF_KWARGS.copy()
        mode = kw.pop('mode', None)
        fixed = cls._fix_data(df)
        fixed = cls._ensure_index(fixed, level)
        if mode in ('w', 'r+'):
            raise ValueError(f"不可使用`w`模式。如需覆盖式写入，请使用`put`方法")
        kw_.update(kw)
        # min_itemsize是一个恼人的问题
        if level:
            kw_.update({'min_itemsize': DB_CONFIG[level]['min_itemsize']})
        key = 'df' if level is None else f"{level}/df"
        with cls.get_store() as store:
            # index设置为False加快写入速度
            store.append(key, fixed, index=False, **kw_)

    @classmethod
    def remove(cls, key, where=None):
        """部分移除表中符合条件的数据行"""
        with cls.get_store() as store:
            store.remove(key, where)

    @staticmethod
    def build_period_stmt(start, end, dt_col):
        """构造期间查询语句（演示）"""
        return _period_query_stmt(start, end, dt_col)

    @classmethod
    def query(cls):
        raise NotImplementedError('子类中完成')

    @classmethod
    def verify_level(cls, level):
        # DataBrowseStore专用。其他类使用level会导致严重错误
        c_name = cls.__name__
        if c_name.upper().startswith('TEST'):
            return
        if c_name not in ("DataBrowseStore",) and level:
            raise ValueError("level为DataBrowseStore专用参数")
        if c_name in ("DataBrowseStore",):
            if level is None:
                raise ValueError("必须指定level")
            assert level in DB_CONFIG.keys(), f"可用项目{DB_CONFIG.keys()}"

    def __enter__(self):
        name = self.FP_NAME
        if is_using(name):
            raise ForbidPparallel(f'Pytables不支持并行操作。容易导致数据损坏。数据库：{name}。')
        mark_use(name)
        return self

    def __exit__(self, *args):
        name = self.FP_NAME
        mark_end(name)

    def close(self):
        name = self.FP_NAME
        mark_end(name)


class TimeStore(StoreBase):
    doc = """"仅以时间为索引的数据工具类\n\n"""
    __doc__ = doc + __USAGE__

    @classmethod
    def query(cls, start=None, end=None, where=None, iterator=False):
        """期间条件查询

        Keyword Arguments:
            start {Timestamp} -- 开始日期 (default: {None})
            end {Timestamp} -- 结束时间 (default: {None})
            where {expr} -- 查询表达式 (default: {None})
            iterator {bool} -- 是否迭代输出 (default: {False})
                预计数据量大时，须使用迭代输出

        Raises:
            ValueError: 查询条件表达式只能为str或list of str

        Returns:
            DataFrame -- 数据框或迭代
        """
        # 单索引其名称固定为`index`!!!
        dt_col = 'index'
        # 首先组合时期与附加条件
        p1 = _period_query_stmt(start, end, dt_col)
        if p1 and where:
            # 附加其他查询条件，其连接关系只能为`&`
            if isinstance(where, str):
                where = ' & '.join([p1, where])
            elif isinstance(where, list):
                where = ' & '.join([p1] + where)
            else:
                raise ValueError('附加其他查询条件[where]，其表达式要么为str要么为list of str')
        elif p1:
            where = p1
        name = cls.FP_NAME
        return _query(name, where, iterator)


class StockStore(StoreBase):

    __header__ = "以时间、代码组合为索引的数据\n\n"

    __doc__ = __header__ + __USAGE__

    @classmethod
    def query(cls, level=None, codes=None, start=None, end=None, where=None, iterator=False):
        """查询项目期间数据

        Keyword Arguments:
            level {str} -- 项目层级。如`2.1`
            codes {list} -- 查询股票代码列表 (default: {None})
            start {Timestamp} -- 开始日期 (default: {None})
            end {Timestamp} -- 结束时间 (default: {None})
            where {expr} -- 查询表达式 (default: {None})
            iterator {bool} -- 是否迭代输出 (default: {False})
                预计返回数据量非常大时，使用迭代输出减少内存占用

        Raises:
            ValueError: 查询条件表达式只能为str或list of str

        Returns:
            DataFrame -- 数据框或迭代
        """
        cls.verify_level(level)
        dt_col = cls.get_dt_col(level)
        code_col = cls.get_code_col(level)
        # 字符串 -> list of str
        if isinstance(codes, str):
            codes = [codes]
        # 首先组合时期与附加条件
        p1 = _period_query_stmt(start, end, dt_col)
        if p1 and where:
            # 附加其他查询条件，其连接关系只能为`&`
            if isinstance(where, str):
                where = ' & '.join([p1, where])
            elif isinstance(where, list):
                where = ' & '.join([p1] + where)
            else:
                raise ValueError('附加其他查询条件[where]，其表达式要么为str要么为list of str')
        elif p1:
            where = p1
        c1 = _codes_query_stmt(codes, code_col)
        if c1 and where:
            # 添加`()`分组符号
            where = f"{where} & ({c1}) "
        elif c1:
            where = c1
        name = cls.FP_NAME
        key = f"{level}/df" if level else "df"
        return _query(name, where, iterator, key)

    @classmethod
    def get_min_and_max_index(cls, code, level=None):
        """数据中指定代码时间索引最小、最大值

        Arguments:
            code {str} -- 股票代码

        Returns:
            tuple -- 最小时间、最大时间二元组
        """
        cls.verify_level(level)
        df = cls.query(level, [code])
        dt_col = cls.get_dt_col(level)
        df = df.reset_index()
        min_dt = df[dt_col].min()
        max_dt = df[dt_col].max()
        min_dt = MARKET_START.tz_localize(None) if pd.isna(min_dt) else min_dt
        # max_dt = pd.Timestamp.now().normalize() if pd.isna(max_dt) else max_dt
        max_dt = MARKET_START.tz_localize(None) if pd.isna(max_dt) else max_dt
        return min_dt, max_dt

    @classmethod
    def update_record(cls, level):
        """更新股票现有数据中的开始日期、结束日期记录
        """
        cls.verify_level(level)
        dt_col = cls.get_dt_col(level)
        code_col = cls.get_code_col(level)
        record = defaultdict(dict)
        min_dts = defaultdict(list)
        max_dts = defaultdict(list)

        for df in cls.query(level, iterator=True):
            data = df.reset_index()
            s = data.groupby(code_col)[dt_col]
            mins = s.min().to_dict()
            maxs = s.max().to_dict()
            for code, v in mins.items():
                min_dts[code].append(v)
            for code, v in maxs.items():
                max_dts[code].append(v)
        for code, vs in min_dts.items():
            record[code]['min_dt'] = min(vs)
        for code, vs in max_dts.items():
            record[code]['max_dt'] = max(vs)

        key = f"{level}/record" if level else "record"
        data = pd.DataFrame.from_dict(record).T
        cls.put(data.sort_index(), key)


class TradingDateStore(TimeStore):
    """交易日期"""

    FP_NAME = 'trading_dates'
    INDEX_COL = ('交易日期', None)

    @classmethod
    def _fix_data(cls, df):
        df['交易日期'] = df['trading_date']
        return df

    @classmethod
    def get_data(cls):
        return cls.query()['trading_date'].tolist()


class TreasuryDateStore(TimeStore):
    """国库券利率"""

    FP_NAME = 'treasury'
    INDEX_COL = ('date', None)


class SinaNewsStore(TimeStore):
    """新浪财经新闻"""

    FP_NAME = 'sina_news'
    INDEX_COL = ('时间', None)


class WyCjmxStore(StockStore):
    """网易股票成交明细"""

    FP_NAME = 'wy_cjmx'
    INDEX_COL = ('成交时间', '股票代码')


class WyStockDailyStore(StockStore):
    """网易股票日线数据"""

    FP_NAME = 'wy_stock'
    INDEX_COL = ('日期', '股票代码')


class WyIndexDailyStore(StockStore):
    """网易指数日线数据"""

    FP_NAME = 'wy_index'
    INDEX_COL = ('日期', '股票代码')


class SinaQuotesStore(StockStore):
    """新浪实时报价"""

    FP_NAME = 'sina_quotes'
    INDEX_COL = ('时间', '股票代码')


class TctMinutelyStore(StockStore):
    """腾讯分时成交"""

    FP_NAME = 'tct_minute'
    INDEX_COL = ('时间', '股票代码')


class MarginStore(StockStore):
    """融资融券数据"""

    FP_NAME = 'margin'
    INDEX_COL = ('交易日期', '股票代码')


class DisclosureStore(StockStore):
    """公司公告"""

    FP_NAME = 'disclosure'
    INDEX_COL = ('公告时间', '股票代码')


class IndexStore(StoreBase):
    """自然序号为索引的数据工具类"""

    INDEX_COL = (None, None)

    @staticmethod
    def available_names():
        return {
            'classify_tree': '股票分类树',
            'classify_bom': '分类基础表',
            'ths_gn': '同花顺概念'
        }

    @classmethod
    def _ensure_index(cls, df, level):
        """设置索引"""
        # 无需处理
        return df

    @classmethod
    def query(cls, codes=None, where=None, iterator=False):
        """查询指定代码列表的数据

        Keyword Arguments:
            codes {list} -- 查询股票代码列表 (default: {None})
            where {expr} -- 查询表达式 (default: {None})
            iterator {bool} -- 是否迭代输出 (default: {False})
                预计数据量大时，须使用迭代输出

        Raises:
            ValueError: 查询条件表达式只能为str或list of str

        Returns:
            DataFrame -- 数据框或迭代
        """
        code_col = cls.get_code_col(None)
        c1 = _codes_query_stmt(codes, code_col)
        if c1 and where:
            # 添加`()`分组符号
            where = f"{where} & ({c1}) "
        elif c1:
            where = c1
        name = cls.FP_NAME
        return _query(name, where, iterator)


class ClassifyTreeStore(IndexStore):
    """股票分类树"""

    FP_NAME = 'classify_tree'

    @classmethod
    def get_bom(cls):
        """获取同花顺概念综述表"""
        with cls.get_store() as store:
            return store.get('bom')


class ThsGnStore(IndexStore):
    """同花顺概念表"""

    FP_NAME = 'ths_gn'

    @classmethod
    def get_gn_time(cls):
        """获取同花顺概念综述表"""
        with cls.get_store() as store:
            return store.get('time')


class TctGnStore(IndexStore):
    """腾讯概念表"""

    FP_NAME = 'tct_gn'


class DataBrowseStore(StockStore):
    """深证信数据浏览store

    Usage:
    >>> # 基础信息
    >>> DataBrowseStore.query('1')
    >>> # 个股TTM财务利润表
    >>> DataBrowseStore.query('7.4.1',['000001'],'2019-01-01')
    """

    FP_NAME = 'data_browse'

    @classmethod
    def get_dt_col(cls, level):
        """获取序列时间列名称"""
        try:
            return DB_CONFIG[level]['key_columns'][1]
        except IndexError:
            return None

    @classmethod
    def get_code_col(cls, level):
        """获取序列代码列名称"""
        return DB_CONFIG[level]['key_columns'][0]

    @classmethod
    def get_key_columns(cls, level):
        """获取组合主键列表"""
        return DB_CONFIG[level]['key_columns']

    @classmethod
    def drop_duplicates(cls, level, subset=None):
        """清除重复项"""
        # 读取、覆盖
        if subset is None:
            subset = cls.get_key_columns(level)
        df = cls.query(level)
        data = df.reset_index().drop_duplicates(subset)
        cls.remove(level)
        cls.append(data, level=level)
        cls.create_table_index(level)

    @classmethod
    def remove(cls, level, where=None):
        """移除表对应层级中符合条件的数据行

        Arguments:
            level {str} -- 数据层

        Keyword Arguments:
            where {str or list of str} -- 条件语句 (default: {None})
        """
        assert level in DB_CONFIG.keys(), f"可选层级{DB_CONFIG.keys()}"
        key = f"{level}/df"
        with cls.get_store() as store:
            store.remove(key, where)


class TestStore(WyCjmxStore):
    """测试用"""
    FP_NAME = 't2'
