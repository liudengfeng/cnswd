"""
命令行

用法
# 查看使用方法
$ stock --help
# 刷新<深证信股指日线>数据
$ stock szxs

使用后台任务计划，自动刷新数据

"""
import asyncio

import click
import pandas as pd

from ..setting.config import DB_CONFIG
from ..store import TreasuryDateStore
from ..utils import kill_firefox, remove_temp_files
from ..websource.treasuries import download_last_year, fetch_treasury_data_from
from . import (data_backup, refresh_asr, refresh_classify, refresh_disclosure,
               refresh_margin, refresh_quote, refresh_sina_news,
               refresh_tct_gn, refresh_tct_minutely, refresh_ths_gn,
               ptrepack,
               refresh_wy_cjmx, refresh_wyi, refresh_wys)
from .trading_calendar import refresh_trading_calendar


@click.group()
def stock():
    """
    刷新股票数据\n
    \t1. 基本维护\n
    \t2. 基础信息\n
    \t3. 交易数据\n
    """
    pass


# region 新浪

@stock.command()
@click.option('--pages', default=3, help='刷新页数')
def news(pages):
    """【新浪】财经消息"""
    refresh_sina_news.refresh(pages)


@stock.command()
def quote():
    """【新浪】股票实时报价"""
    asyncio.run(refresh_quote.refresh())

# endregion


# region 巨潮

@stock.command()
def disclosure():
    """【巨潮】刷新公司公告"""
    asyncio.run(refresh_disclosure.refresh())


@stock.command()
def margin():
    """刷新【深证信】融资融券"""
    refresh_margin.refresh()


@stock.command()
def classify():
    """【深证信】股票分类及BOM表"""
    refresh_classify.refresh()


@stock.command()
@click.option(
    '--levels',
    required=False,
    multiple=True,
    default=list(DB_CONFIG.keys()),
    type=click.Choice(list(DB_CONFIG.keys()), case_sensitive=False),
    help='深证信高级搜索项目数据。指定多项目 stock asr --levels=1 --levels=2.1 全部项目 stock asr',
)
def asr(levels):
    """刷新【深证信】数据浏览项目数据"""
    refresh_asr.refresh(levels)

# endregion


# region 网易

@stock.command()
def wys():
    """刷新【网易】股票日线数据"""
    refresh_wys.refresh()


@stock.command()
def wyi():
    """刷新【网易】股票指数日线数据"""
    refresh_wyi.refresh()


@stock.command()
def calendar():
    """交易日历【网易】"""
    refresh_trading_calendar()


@stock.command()
def cjmx():
    """刷新【网易】近期成交明细"""
    refresh_wy_cjmx.refresh_last_5()

# endregion

# region 同花顺


@stock.command()
def thsgn():
    """刷新【同花顺】概念股票列表"""
    refresh_ths_gn.refresh()

# endregion

# region 腾讯


@stock.command()
def tctgn():
    """刷新【腾讯】概念股票列表"""
    refresh_tct_gn.refresh()


@stock.command()
def tctm():
    """刷新【腾讯】分钟级别交易数据"""
    refresh_tct_minutely.refresh()

# endregion

# region 其他网站


@stock.command()
def treasury():
    """刷新国库券利率数据"""
    download_last_year()
    df = fetch_treasury_data_from()
    TreasuryDateStore.put(df, 'df')
    print('done')


# endregion


# region 其他辅助命令


@stock.command()
def backup():
    """数据备份"""
    data_backup.backup()


@stock.command()
def clean():
    """清理临时数据"""
    remove_temp_files()
    kill_firefox()


@stock.command()
def comp():
    """压缩数据"""
    ptrepack.main()

# endregion
