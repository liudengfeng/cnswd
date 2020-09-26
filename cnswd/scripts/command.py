"""
命令行

用法
# 查看使用方法
$ stock --help
# 刷新网易股票指数数据
$ stock wyi

可使用后台任务计划，自动刷新数据

"""
import asyncio

import click
import pandas as pd

from ..cninfo import ASR_KEYS
from ..utils import kill_firefox, remove_temp_files
from . import (classify, cninfo, cninfo_meta, cninfo_yypl, disclosure,
               index_codes, sina_margin, sina_news, sina_quote,
               sina_quote_index, sina_tzpj, sw_class, tct_gn, tct_minutely,
               ths_gn, ths_news, trading_calendar, trading_codes, treasury,
               wy_cjmx, wy_cwbg, wy_fhpg, wy_gszl, wy_index, wy_quote,
               wy_quote_index, wy_stock, wy_yjyg, wy_zycwzb, yahoo)


@click.group()
def stock():
    """
    股票数据管理工具\n
    \t1. 市场数据\n
    \t2. 财经消息\n
    \t3. 股票交易数据\n
    \t4. 财务报告及指标\n
    \t5. 数据整理工具\n
    """
    pass


@stock.command()
def yypl():
    """【巨潮】财报预约披露日期"""
    cninfo_yypl.refresh()


# endregion


# region 市场数据
@stock.command()
def trs():
    """刷新国库券利率数据"""
    treasury.refresh()


@stock.command()
def cld():
    """交易日历【网易】"""
    trading_calendar.refresh()


@stock.command()
def codes():
    """股票代码列表"""
    trading_codes.refresh()


@stock.command()
def icodes():
    """指数代码列表"""
    index_codes.refresh()


@stock.command()
def clsf():
    """【深证信】股票分类及BOM表"""
    classify.refresh()


@stock.command()
def tctgn():
    """刷新【腾讯】概念股票列表"""
    tct_gn.refresh()


@stock.command()
def thsgn():
    """刷新【同花顺】概念股票列表"""
    ths_gn.refresh()


# endregion

# region 财经消息及公告


@stock.command()
@click.option('--pages', default=3, help='刷新总页数')
def snnews(pages):
    """【新浪】财经消息"""
    sina_news.refresh(pages)


@stock.command()
def sntzpj():
    """【新浪】最新股票评级"""
    sina_tzpj.refresh()


@stock.command()
def snrzrq():
    """【新浪】融资融券"""
    sina_margin.refresh()


@stock.command()
@click.option('--pages', default=3, help='刷新总页数')
@click.option('--init', is_flag=True, help='是否初始化')
def thsnews(pages, init):
    """【同花顺】财经消息"""
    # before_refresh()
    if init:
        asyncio.run(ths_news.refresh(pages, True))
    else:
        asyncio.run(ths_news.refresh(pages, False))


@stock.command()
@click.option('--init', is_flag=True, help='是否初始化')
def dscl(init):
    """【巨潮】刷新公司公告"""
    # before_refresh()
    asyncio.run(disclosure.refresh(init))


# endregion

# region 交易数据


@stock.command()
def wyquote():
    """【网易】股票实时报价"""
    wy_quote.refresh()


@stock.command()
def wyiquote():
    """【网易】股票指数实时报价"""
    wy_quote_index.refresh()


@stock.command()
def quote():
    """【新浪】股票实时报价"""
    asyncio.run(sina_quote.refresh())


@stock.command()
def iquote():
    """【新浪】指数实时报价"""
    asyncio.run(sina_quote_index.refresh())


# @stock.command()
# def margin():
#     """刷新【深证信】融资融券"""
#     # before_refresh()
#     cninfo.refresh_margin()


@stock.command()
def wys():
    """刷新【网易】股票日线数据"""
    # before_refresh()
    wy_stock.refresh()


@stock.command()
def wyi():
    """刷新【网易】股票指数日线数据"""
    # before_refresh()
    wy_index.refresh()


@stock.command()
def wyfhpg():
    """刷新【网易】股票分红配股数据"""
    wy_fhpg.refresh()


@stock.command()
def wycwbg():
    """刷新【网易】股票财务报告三表"""
    wy_cwbg.refresh()


@stock.command()
def wyzycwzb():
    """刷新【网易】股票财务主要财务指标"""
    wy_zycwzb.refresh()


@stock.command()
def wyyjyg():
    """刷新【网易】股票业绩预告"""
    wy_yjyg.refresh()


@stock.command()
def wygszl():
    """刷新【网易】公司简介"""
    wy_gszl.refresh()


@stock.command()
def cjmx():
    """刷新【网易】近期成交明细"""
    # before_refresh()
    wy_cjmx.refresh_last_5()


@stock.command()
def tctm():
    """刷新【腾讯】分钟级别交易数据"""
    # before_refresh()
    tct_minutely.refresh()


# endregion


# region 财报及指标
# @stock.command()
# @click.option(
#     '--items',
#     required=False,
#     multiple=True,
#     default=ASR_KEYS,
#     type=click.Choice(ASR_KEYS, case_sensitive=False),
#     help='深证信高级搜索项目数据。指定多项目 stock asr --items=基本资料 --items=个股报告期利润表 \n 全部项目 stock asr',
# )
# def asr(items):
#     """刷新【深证信】数据浏览项目数据"""
#     # before_refresh()
#     # click.echo(items)
#     cninfo.refresh_asr(items)


@stock.command()
def yh():
    """刷新【雅虎】财经数据"""
    yahoo.refresh()


@stock.command()
def swclass():
    """刷新【申万】行业分类"""
    sw_class.refresh()


# endregion

# region 其他辅助命令


@stock.command()
def clean():
    """清理临时数据"""
    remove_temp_files()
    kill_firefox()


# endregion
