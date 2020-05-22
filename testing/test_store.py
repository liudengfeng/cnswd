"""

对本地数据进行测试验证

重要：
    1. 必须提取网络数据到本地
    2. 如在刷新数据期间测试，否则极可能损坏数据。
       尽管可直接使用类方法查询数据，但为保护数据不因异常中断损害，使用with
"""

import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_equal
from cnswd.store import (ClassifyTreeStore, DataBrowseStore, MarginStore,
                         SinaNewsStore, TradingDateStore, TreasuryDateStore,
                         TctMinutelyStore, SinaQuotesStore,
                         WyCjmxStore, WyIndexDailyStore, WyStockDailyStore)


def test_read_margin():
    """测试读取融资融券"""
    start = '2020-04-02'
    end = start
    with MarginStore() as store:
        df = store.query(start=start, end=end)
        assert len(df) == 1691


def test_read_news():
    """测试读取财经新闻"""
    start = pd.Timestamp('2020-04-05 12:00:00')
    end = start + pd.Timedelta(minutes=30)
    with SinaNewsStore() as store:
        df = store.query(start, end)
        assert len(df) == 7


def test_read_treasury():
    """"测试读取国库券利率"""
    start = '2005-01-01'
    end = '2019-12-31'
    with TreasuryDateStore() as store:
        df = store.query(start, end)
        assert df.shape == (3729, 16)


def test_read_classify_tree():
    """测试读取股票分类树"""
    with ClassifyTreeStore() as store:
        df = store.query()
        assert df[df['分类名称'] == '国证2000'].shape == (2000, 7)
        assert df[df['分类名称'] == '沪市A'].shape[0] > 1492
        # 当前数据应该有9行
        assert df[df['分类层级'] == '3.1.1.1.1'].shape[0] > 7


def test_read_stock_list():
    """测试读取股票列表"""
    with DataBrowseStore() as store:
        df = DataBrowseStore.query('1')
        assert len(df) > 3800


def test_szx_daily_history():
    """测试股票日线行情"""
    with WyStockDailyStore() as store:
        df = WyStockDailyStore.query(
            None, ['000001'], '2019-12-01', '2019-12-19')
        assert df.shape == (14, 14)


def test_trading_dates():
    """测试交易日历"""
    start = pd.Timestamp('2000-01-01')
    end = pd.Timestamp('2019-12-23')
    with TradingDateStore() as store1, WyIndexDailyStore() as store2:
        actual = store1.query(start, end)['trading_date'].values
        expected = store2.query(
            None, ['399001'], start, end).index.get_level_values(0).values
        assert_array_equal(actual, sorted(expected))


@pytest.mark.parametrize("code, date, expected_rows", [
    ('600000', '2020-04-02', 2254),
    ('000001', '2020-04-02', 4127),
])
def test_cjmx(code, date, expected_rows):
    """测试读取成交明细

    只能提取最近一周的成交明细
    """
    with WyCjmxStore() as store:
        df = store.query(None, code, date, date)
        assert len(df) == expected_rows


def test_DataBrowseStore():
    # 不能使用并行测试!!!
    paras = [
        ('2.1', '2019-04-01', '2019-06-30', 110),
        ('2.2', '2019-04-01', '2019-06-30', 5175),
        # 公告日期
        ('2.3', '2019-04-01', '2019-06-30', 3449),
        ('2.4', '2019-04-01', '2019-06-30', 5429),
        ('2.5', '2019-04-01', '2019-06-30', 3737),
        ('3', '2019-04-01', '2019-06-30', 17682),
        ('4.1', '2019-04-01', '2019-06-30', 1905),
        # 全年
        ('5', '2018-01-01', '2018-12-31', 7322),
        # 期间
        ('6.1', '2019-04-01', '2019-06-30', 181),
        ('6.2', '2019-04-01', '2019-06-30', 134),
        ('6.3', '2019-04-01', '2019-06-30', 12),
        ('6.4', '2019-04-01', '2019-06-30', 5),
        # 季度财报
        ('7.1.1', '2019-04-01', '2019-06-30', 3895),
        ('7.1.2', '2019-04-01', '2019-06-30', 3895),
        # 网站bug 三张表应该一致 行数统一才对
        ('7.1.3', '2019-04-01', '2019-06-30', 3894),
        ('7.2.1', '2019-04-01', '2019-06-30', 3893),
        ('7.2.2', '2019-04-01', '2019-06-30', 3946),
        ('7.3.1', '2019-04-01', '2019-06-30', 3692),
        ('7.3.2', '2019-04-01', '2019-06-30', 3692),
        ('7.3.3', '2019-04-01', '2019-06-30', 3692),
        ('7.4.1', '2019-04-01', '2019-06-30', 3737),
        ('7.4.2', '2019-04-01', '2019-06-30', 3737),
    ]
    with DataBrowseStore() as store:
        for level, start, end, expected_rows in paras:
            actual = store.query(level, None, start, end)
            assert len(actual) == expected_rows
