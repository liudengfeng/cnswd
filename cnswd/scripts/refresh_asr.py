"""

刷新深证信数据搜索

# 7.1.4 ~ 7.1.6 2007金融版本舍弃

关键点：
1. 时间重叠问题 设置回退天数
2. 新增代码补齐：以IPO为基准日期

"""
import time
import warnings

import pandas as pd
from numpy.random import shuffle
from pandas.tseries.offsets import QuarterEnd, YearBegin


from .._exceptions import RetryException, FutureDate
from ..cninfo import AdvanceSearcher, FastSearcher
from ..cninfo.utils import get_field_type, get_min_itemsize
from ..setting.config import DB_CONFIG
from ..setting.constants import MARKET_START
from ..store import DataBrowseStore
from ..utils import ensure_dtypes, make_logger, time_for_next_update


warnings.filterwarnings('ignore')


class ASRefresher(object):
    """深证信数据刷新器

    下载网络数据，比较、更新，将新增部分添加存储到本地数据库。
    """
    def __new__(cls):
        cls._fs_api = None  # 快速搜索
        cls._as_api = None  # 高级搜索
        cls.store = DataBrowseStore()
        cls._current_api = None
        cls.logger = make_logger('深证信')
        return super(ASRefresher, cls).__new__(cls)

    # def __init__(self):
    #     self._fs_api = None  # 快速搜索
    #     self._as_api = None  # 高级搜索
    #     self.store = DataBrowseStore()
    #     self._current_api = None
    #     self.logger = make_logger('深证信')

    @classmethod
    def get_min_itemsize(cls, level):
        """定义列字符串最小长度"""
        try:
            return DB_CONFIG[level]['min_itemsize']
        except KeyError:
            return {}

    @classmethod
    def get_freq(cls, level):
        """刷新频率"""
        try:
            # 第二个字符
            freq = DB_CONFIG[level]['date_freq'][0][1]
            return freq
        except Exception:
            return None

    @classmethod
    def get_min_date(cls, level):
        """项目最初日期"""
        min_start = DB_CONFIG[level]['date_field'][1]
        if min_start is None:
            return None
        return pd.Timestamp(min_start)

    @classmethod
    def get_ipo_date(cls, code):
        """查询股票上市日期"""
        try:
            df = cls.store.query('1', code)
            return df.index.get_level_values(0)[0]
        except KeyError:
            msg = '首先刷新股票基本资料(1)，才能提供上市日期信息，运行：\n stock asr --levels 1'
            warnings.warn(msg)
        except Exception:
            pass
        return None

    @classmethod
    def get_daily_start_date(cls, level):
        """获取每日更新项目的开始刷新日期"""
        freq = cls.get_freq(level)
        assert freq == 'D'
        try:
            # 首先搜索record记录
            record_key = f"{level}/record"
            record = cls.store.get(record_key)
            # 防止遗漏数据，在原基础上减去一天
            return record['max_dt'].max() - pd.Timedelta(days=1)
        except Exception:
            # 然后项目最初日期
            return cls.get_min_date(level)
        return MARKET_START.tz_localize(None)

    @classmethod
    def get_quarterly_start_date(cls, level):
        """获取季度更新项目的开始刷新日期"""
        freq = cls.get_freq(level)
        assert freq == 'Q'
        try:
            # 首先搜索record记录
            record_key = f"{level}/record"
            record = cls.store.get(record_key)
            offset = QuarterEnd(n=-2, startingMonth=3, normalize=True)
            # 防止遗漏数据，在原基础上减去二季度
            return offset.apply(record['max_dt'].max())
        except Exception:
            # 然后项目最初日期
            return cls.get_min_date(level)
        return MARKET_START.tz_localize(None)

    @classmethod
    def get_yearly_start_date(cls, level):
        """获取年度更新项目的开始刷新日期"""
        freq = cls.get_freq(level)
        assert freq == 'Y'
        try:
            # 首先搜索record记录
            record_key = f"{level}/record"
            record = cls.store.get(record_key)
            offset = YearBegin(n=-2, month=1, normalize=True)
            # 防止遗漏数据，在原基础上调整为上年1月1日
            return offset.apply(record['max_dt'].max())
        except Exception:
            # 然后项目最初日期
            return cls.get_min_date(level)
        return MARKET_START.tz_localize(None)

    def _append_data_after(self, level, start):
        """自开始日期添加层网络数据"""
        today = pd.Timestamp.now().normalize()
        if start > today:
            return

        col_dtypes = get_field_type('db', level)

        web_df = self._as_api.get_data(level, start)
        web_df = ensure_dtypes(web_df, **col_dtypes)

        # 此时删除本地数据
        dt_col = self.store.get_dt_col(level)
        where = f"{dt_col} >= {start!r}"

        try:
            self.store.remove(level, where)
        except KeyError:
            pass

        # 添加数据
        self.store.append(web_df, level=level)
        self.logger.info(f"{level} 添加 {start}起 {len(web_df)}行")

    def _overwrite(self, level):
        """与本地数据合并后重写"""
        api = self._as_api
        try:
            local_df = self.store.query(level)
            local_df.rest_index(inplace=True)
        except Exception:
            local_df = pd.DataFrame()
        subset = self.store.get_code_col(level)
        web_df = api.get_data(level)
        col_dtypes = get_field_type('db', level)
        web_df = ensure_dtypes(web_df, **col_dtypes)
        df = pd.concat([local_df, web_df])
        df.drop_duplicates(subset, inplace=True)
        # 写入前，清除本地数据
        try:
            self.store.remove(level)
        except KeyError:
            pass

        self.store.append(web_df, level=level)

    def get_new_codes(self, level, web_codes):
        """网络股票代码、本地股票代码之差"""
        record_key = f"{level}/record"
        try:
            record = self.store.get(record_key)
            local_codes = record.index.values.tolist()
        except KeyError:
            local_codes = []
        if len(local_codes) == 0:
            new_codes = []
        else:
            # 差集
            new_codes = list(set(web_codes).difference(set(local_codes)))
        return new_codes

    def set_refresh_time(self, level, code, dt=pd.Timestamp.now()):
        """设置项目股票刷新时间"""
        key = f"{level}/refresh_time"
        try:
            s = self.store.get(key)
        except Exception:
            s = pd.Series()
        s[code] = dt
        s.sort_index(inplace=True)
        self.store.put(s, key)

    def get_refresh_time(self, level, code):
        """获取项目股票刷新时间"""
        key = f"{level}/refresh_time"
        try:
            return self.store.get(key)[code]
        except Exception:
            return MARKET_START.tz_localize(None)

    def set_level_refresh_time(self, level):
        """设置项目刷新时间"""
        now = pd.Timestamp.now()
        self.store.set_attr('level_refresh_time', now, level)

    def get_level_refresh_time(self, level):
        """获取项目刷新时间"""
        default = MARKET_START.tz_localize(None)
        dt = self.store.get_attr('level_refresh_time', default, level)
        return dt

    def _append_data_before(self, level, codes, freq, end):
        """为层添加给定代码至结束日期止的网络数据"""
        self.logger.info(f"初始化刷新：{len(codes)}只股票")
        code_col = self.store.get_code_col(level)
        col_dtypes = get_field_type('db', level)
        fetch_data_func = self._fs_api.get_data
        self._current_api = 'fs'

        for code in codes:
            ipo = self.get_ipo_date(code)
            # ipo时间为空，无法取得网络数据
            if ipo is None:
                continue
            # 股票部分项目根本没有数据，或者极少发生
            # 记录在案的record只代表本地数据中最小时间与最大时间
            # 为防止股票多次无效重复，使用实际完成刷新时间管理
            available, next_update = self.web_data_available(level, freq, code)

            if not available:
                self.logger.info(
                    f"项目：{level} 股票 {code} 无数据可更新。下次更新时间：{next_update}")
                continue

            # 财务报告类在上市日期前三年
            if level.startswith('7.'):
                start = ipo - pd.Timedelta(days=3 * 365)
            else:
                start = next_update

            try:
                # 获取整理网络数据
                web_df = fetch_data_func(level, code, start, end)
            except (RetryException, FutureDate):
                # 当出现重试异常时，跳过。待下一次刷新时完成。
                # 或者出现未来日期
                continue

            nrows = len(web_df)
            if nrows:
                web_df = ensure_dtypes(web_df, **col_dtypes)
                # 添加网络数据
                # web_df后续为就地修改
                self.store.append(web_df, level=level)
                self.logger.info(f"{level} {code} 添加 {nrows}行")

            # 记录股票数据刷新时间
            self.set_refresh_time(level, code)

    def web_data_available(self, level, freq, code=None):
        """项目数据是否有可用的更新"""
        now = pd.Timestamp.now()
        if freq is None:
            last_refresh_time = self.get_level_refresh_time(level)
        else:
            if code is None:
                last_refresh_time = self.get_level_refresh_time(level)
            else:
                last_refresh_time = self.get_refresh_time(level, code)
        # 默认为D
        freq = 'D' if freq is None else freq
        next_update = time_for_next_update(last_refresh_time, freq)
        if now >= next_update:
            return True, next_update
        else:
            return False, next_update

    def _factory(self, level, web_codes):
        freq = self.get_freq(level)
        # 对项目而言，即使单个股票的更新周期为`Q`或`Y`，项目整体均为每天刷新
        available, next_update = self.web_data_available(level, 'D')
        if not available:
            self.logger.info(f"项目：{level} 无数据可更新。下次更新时间：{next_update}")
            return

        new_codes = self.get_new_codes(level, web_codes)

        if freq is None:
            self._overwrite(level)
        elif freq == 'D':
            start = self.get_daily_start_date(level)
        elif freq == 'Q':
            start = self.get_quarterly_start_date(level)
        elif freq == 'Y':
            start = self.get_yearly_start_date(level)
        else:
            raise ValueError(f'不支持刷新频率`{freq}`')

        # 假设
        # 1. 总体 1,2,3
        # 2. 开始日期 2020-01-01
        # 3. 当前日期 2020-03-31
        # 4. 相对本地数据而言，新增代码 3
        # 上述完成 1,2,3 2020-01-01 ~ 2020-03-31      数据刷新
        # 以下补充     3 ipo date   ~ 2020-01-01 - 1d
        if freq in ('D', 'Q', 'Y'):
            self._append_data_after(level, start)
            # 单独处理新增代码，其结束日期为当前总体代码的开始日期 - 1天
            end = start - pd.Timedelta(days=1)
            self._append_data_before(level, new_codes, freq, end)

        self.store.create_table_index(level)
        self.store.update_record(level)

        # 更新项目刷新时间
        self.set_level_refresh_time(level)

    def init_refresh(self, level, codes):
        """初始化刷新层股票数据(谨慎使用)

        Arguments:
            level {str} -- 数据层
            codes {list} -- 股票代码列表

        Notes:
            清除股票本地数据，重新刷新
        """
        freq = self.get_freq(level)
        if freq not in ('D', 'Q', 'Y'):
            return
        code_col = self.store.get_code_col(level)
        # 删除历史数据
        for code in codes:
            where = f"股票代码 = '{code}'"
            self.store.remove(level, where)
            # 还原刷新时间
            self.set_refresh_time(level, code, MARKET_START.tz_localize(None))
        today = pd.Timestamp.now().normalize()
        self._append_data_before(level, codes, freq, today)

        self.store.create_table_index(level)
        self.store.update_record(level)

    def refresh_batch(self, batch=None):
        """分批刷新

        如批为空，默认刷新全部项目
        """
        if batch is None:
            batch = DB_CONFIG.keys()
        web_codes = self._as_api.codes
        self.logger.info(f"网络股票总量：{len(web_codes)}")
        for level in batch:
            self._factory(level, web_codes)

    def __enter__(self):
        if self._as_api is None:
            self._as_api = AdvanceSearcher()
        if self._fs_api is None:
            self._fs_api = FastSearcher()
        if self.store is None:
            self.store = DataBrowseStore()
        return self

    def __exit__(self, *args):
        if self._as_api and self._as_api.driver:
            self._as_api.driver.quit()
        if self._fs_api and self._fs_api.driver:
            self._fs_api.driver.quit()
        if self.store:
            self.store.close()


def refresh(levels):
    if levels is None:
        levels = DB_CONFIG.keys()
    if isinstance(levels, str):
        # 单项 -> 列表
        levels = [levels]
    with ASRefresher() as f:
        f.refresh_batch(levels)
