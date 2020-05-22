"""

雅虎财经数据偏重于估值与分析

网址
https://finance.yahoo.com/quote/000333.SZ/cash-flow?p=000333.SZ
"""
import re
import requests
import pandas as pd
import time
from pandas.tseries.offsets import DateOffset, BYearEnd, QuarterEnd

TYPE_FIELDS = {
    'annualTotalAssets': 'annualTotalAssets,trailingTotalAssets,annualStockholdersEquity,trailingStockholdersEquity,annualGainsLossesNotAffectingRetainedEarnings,trailingGainsLossesNotAffectingRetainedEarnings,annualRetainedEarnings,trailingRetainedEarnings,annualCapitalStock,trailingCapitalStock,annualTotalLiabilitiesNetMinorityInterest,trailingTotalLiabilitiesNetMinorityInterest,annualTotalNonCurrentLiabilitiesNetMinorityInterest,trailingTotalNonCurrentLiabilitiesNetMinorityInterest,annualOtherNonCurrentLiabilities,trailingOtherNonCurrentLiabilities,annualNonCurrentDeferredRevenue,trailingNonCurrentDeferredRevenue,annualNonCurrentDeferredTaxesLiabilities,trailingNonCurrentDeferredTaxesLiabilities,annualLongTermDebt,trailingLongTermDebt,annualCurrentLiabilities,trailingCurrentLiabilities,annualOtherCurrentLiabilities,trailingOtherCurrentLiabilities,annualCurrentDeferredRevenue,trailingCurrentDeferredRevenue,annualCurrentAccruedExpenses,trailingCurrentAccruedExpenses,annualIncomeTaxPayable,trailingIncomeTaxPayable,annualAccountsPayable,trailingAccountsPayable,annualCurrentDebt,trailingCurrentDebt,annualTotalNonCurrentAssets,trailingTotalNonCurrentAssets,annualOtherNonCurrentAssets,trailingOtherNonCurrentAssets,annualOtherIntangibleAssets,trailingOtherIntangibleAssets,annualGoodwill,trailingGoodwill,annualInvestmentsAndAdvances,trailingInvestmentsAndAdvances,annualNetPPE,trailingNetPPE,annualAccumulatedDepreciation,trailingAccumulatedDepreciation,annualGrossPPE,trailingGrossPPE,annualCurrentAssets,trailingCurrentAssets,annualOtherCurrentAssets,trailingOtherCurrentAssets,annualInventory,trailingInventory,annualAccountsReceivable,trailingAccountsReceivable,annualCashCashEquivalentsAndMarketableSecurities,trailingCashCashEquivalentsAndMarketableSecurities,annualOtherShortTermInvestments,trailingOtherShortTermInvestments,annualCashAndCashEquivalents,trailingCashAndCashEquivalents',
    'annualEbitda': 'annualEbitda,trailingEbitda,annualWeighteAverageShare,trailingWeighteAverageShare,annualDilutedAverageShares,trailingDilutedAverageShares,annualBasicAverageShares,trailingBasicAverageShares,annualearningsPerShare,trailingearningsPerShare,annualDilutedEPS,trailingDilutedEPS,annualBasicEPS,trailingBasicEPS,annualNetIncomeCommonStockholders,trailingNetIncomeCommonStockholders,annualNetIncome,trailingNetIncome,annualNetIncomeContinuousOperations,trailingNetIncomeContinuousOperations,annualTaxProvision,trailingTaxProvision,annualPretaxIncome,trailingPretaxIncome,annualOtherIncomeExpense,trailingOtherIncomeExpense,annualInterestExpense,trailingInterestExpense,annualOperatingIncome,trailingOperatingIncome,annualOperatingExpense,trailingOperatingExpense,annualSellingGeneralAndAdministration,trailingSellingGeneralAndAdministration,annualResearchAndDevelopment,trailingResearchAndDevelopment,annualGrossProfit,trailingGrossProfit,annualCostOfRevenue,trailingCostOfRevenue,annualTotalRevenue,trailingTotalRevenue',
    'annualFreeCashFlow': 'annualFreeCashFlow,trailingFreeCashFlow,annualCapitalExpenditure,trailingCapitalExpenditure,annualOperatingCashFlow,trailingOperatingCashFlow,annualEndCashPosition,trailingEndCashPosition,annualBeginningCashPosition,trailingBeginningCashPosition,annualChangeInCashSupplementalAsReported,trailingChangeInCashSupplementalAsReported,annualCashFlowFromContinuingFinancingActivities,trailingCashFlowFromContinuingFinancingActivities,annualNetOtherFinancingCharges,trailingNetOtherFinancingCharges,annualCashDividendsP…usiness,annualOtherNonCashItems,trailingOtherNonCashItems,annualChangeInAccountPayable,trailingChangeInAccountPayable,annualChangeInInventory,trailingChangeInInventory,annualChangesInAccountReceivables,trailingChangesInAccountReceivables,annualChangeInWorkingCapital,trailingChangeInWorkingCapital,annualStockBasedCompensation,trailingStockBasedCompensation,annualDeferredIncomeTax,trailingDeferredIncomeTax,annualDepreciationAndAmortization,trailingDepreciationAndAmortization,annualNetIncome,trailingNetIncome',
    'quarterlyEbitda': 'quarterlyEbitda,trailingEbitda,quarterlyWeighteAverageShare,trailingWeighteAverageShare,quarterlyDilutedAverageShares,trailingDilutedAverageShares,quarterlyBasicAverageShares,trailingBasicAverageShares,quarterlyearningsPerShare,trailingearningsPerShare,quarterlyDilutedEPS,trailingDilutedEPS,quarterlyBasicEPS,trailingBasicEPS,quarterlyNetIncomeCommonStockholders,trailingNetIncomeCommonStockholders,quarterlyNetIncome,trailingNetIncome,quarterlyNetIncomeContinuousOperations,trailingNetIncomeContinuousOperation…yPretaxIncome,trailingPretaxIncome,quarterlyOtherIncomeExpense,trailingOtherIncomeExpense,quarterlyInterestExpense,trailingInterestExpense,quarterlyOperatingIncome,trailingOperatingIncome,quarterlyOperatingExpense,trailingOperatingExpense,quarterlySellingGeneralAndAdministration,trailingSellingGeneralAndAdministration,quarterlyResearchAndDevelopment,trailingResearchAndDevelopment,quarterlyGrossProfit,trailingGrossProfit,quarterlyCostOfRevenue,trailingCostOfRevenue,quarterlyTotalRevenue,trailingTotalRevenue',
    'quarterlyTotalAssets': 'quarterlyTotalAssets,trailingTotalAssets,quarterlyStockholdersEquity,trailingStockholdersEquity,quarterlyGainsLossesNotAffectingRetainedEarnings,trailingGainsLossesNotAffectingRetainedEarnings,quarterlyRetainedEarnings,trailingRetainedEarnings,quarterlyCapitalStock,trailingCapitalStock,quarterlyTotalLiabilitiesNetMinorityInterest,trailingTotalLiabilitiesNetMinorityInterest,quarterlyTotalNonCurrentLiabilitiesNetMinorityInterest,trailingTotalNonCurrentLiabilitiesNetMinorityInterest,quarterlyOtherNonCurrentLia…latedDepreciation,trailingAccumulatedDepreciation,quarterlyGrossPPE,trailingGrossPPE,quarterlyCurrentAssets,trailingCurrentAssets,quarterlyOtherCurrentAssets,trailingOtherCurrentAssets,quarterlyInventory,trailingInventory,quarterlyAccountsReceivable,trailingAccountsReceivable,quarterlyCashCashEquivalentsAndMarketableSecurities,trailingCashCashEquivalentsAndMarketableSecurities,quarterlyOtherShortTermInvestments,trailingOtherShortTermInvestments,quarterlyCashAndCashEquivalents,trailingCashAndCashEquivalents',
    'quarterlyFreeCashFlow': 'quarterlyFreeCashFlow,trailingFreeCashFlow,quarterlyCapitalExpenditure,trailingCapitalExpenditure,quarterlyOperatingCashFlow,trailingOperatingCashFlow,quarterlyEndCashPosition,trailingEndCashPosition,quarterlyBeginningCashPosition,trailingBeginningCashPosition,quarterlyChangeInCashSupplementalAsReported,trailingChangeInCashSupplementalAsReported,quarterlyCashFlowFromContinuingFinancingActivities,trailingCashFlowFromContinuingFinancingActivities,quarterlyNetOtherFinancingCharges,trailingNetOtherFinancingChar…shItems,trailingOtherNonCashItems,quarterlyChangeInAccountPayable,trailingChangeInAccountPayable,quarterlyChangeInInventory,trailingChangeInInventory,quarterlyChangesInAccountReceivables,trailingChangesInAccountReceivables,quarterlyChangeInWorkingCapital,trailingChangeInWorkingCapital,quarterlyStockBasedCompensation,trailingStockBasedCompensation,quarterlyDeferredIncomeTax,trailingDeferredIncomeTax,quarterlyDepreciationAndAmortization,trailingDepreciationAndAmortization,quarterlyNetIncome,trailingNetIncome',
}
# 不包括TTM
TYPE_FIELDS_WITHOUT_TTM = {
    'annualTotalAssets': 'annualTotalAssets,annualStockholdersEquity,annualGainsLossesNotAffectingRetainedEarnings,annualRetainedEarnings,annualCapitalStock,annualTotalLiabilitiesNetMinorityInterest,annualTotalNonCurrentLiabilitiesNetMinorityInterest,annualOtherNonCurrentLiabilities,annualNonCurrentDeferredRevenue,annualNonCurrentDeferredTaxesLiabilities,annualLongTermDebt,annualCurrentLiabilities,annualOtherCurrentLiabilities,annualCurrentDeferredRevenue,annualCurrentAccruedExpenses,annualIncomeTaxPayable,annualAccountsPayable,annualCurrentDebt,annualTotalNonCurrentAssets,annualOtherNonCurrentAssets,annualOtherIntangibleAssets,annualGoodwill,annualInvestmentsAndAdvances,annualNetPPE,annualAccumulatedDepreciation,annualGrossPPE,annualCurrentAssets,annualOtherCurrentAssets,annualInventory,annualAccountsReceivable,annualCashCashEquivalentsAndMarketableSecurities,annualOtherShortTermInvestments,annualCashAndCashEquivalents',
    'annualEbitda': 'annualEbitda,annualWeighteAverageShare,annualDilutedAverageShares,annualBasicAverageShares,annualearningsPerShare,annualDilutedEPS,annualBasicEPS,annualNetIncomeCommonStockholders,annualNetIncome,annualNetIncomeContinuousOperations,annualTaxProvision,annualPretaxIncome,annualOtherIncomeExpense,annualInterestExpense,annualOperatingIncome,annualOperatingExpense,annualSellingGeneralAndAdministration,annualResearchAndDevelopment,annualGrossProfit,annualCostOfRevenue,annualTotalRevenue',
    'annualFreeCashFlow': 'annualFreeCashFlow,annualCapitalExpenditure,annualOperatingCashFlow,annualEndCashPosition,annualBeginningCashPosition,annualChangeInCashSupplementalAsReported,annualCashFlowFromContinuingFinancingActivities,annualNetOtherFinancingCharges,annualCashDividendsP…usiness,annualOtherNonCashItems,annualChangeInAccountPayable,annualChangeInInventory,annualChangesInAccountReceivables,annualChangeInWorkingCapital,annualStockBasedCompensation,annualDeferredIncomeTax,annualDepreciationAndAmortization,annualNetIncome',
    'quarterlyEbitda': 'quarterlyEbitda,quarterlyWeighteAverageShare,quarterlyDilutedAverageShares,quarterlyBasicAverageShares,quarterlyearningsPerShare,quarterlyDilutedEPS,quarterlyBasicEPS,quarterlyNetIncomeCommonStockholders,quarterlyNetIncome,quarterlyNetIncomeContinuousOperations,quarterlyOtherIncomeExpense,quarterlyInterestExpense,quarterlyOperatingIncome,quarterlyOperatingExpense,quarterlySellingGeneralAndAdministration,quarterlyResearchAndDevelopment,quarterlyGrossProfit,quarterlyCostOfRevenue,quarterlyTotalRevenue',
    'quarterlyTotalAssets': 'quarterlyTotalAssets,quarterlyStockholdersEquity,quarterlyGainsLossesNotAffectingRetainedEarnings,quarterlyRetainedEarnings,quarterlyCapitalStock,quarterlyTotalLiabilitiesNetMinorityInterest,quarterlyTotalNonCurrentLiabilitiesNetMinorityInterest,quarterlyOtherNonCurrentLia…latedDepreciation,quarterlyGrossPPE,quarterlyCurrentAssets,quarterlyOtherCurrentAssets,quarterlyInventory,quarterlyAccountsReceivable,quarterlyCashCashEquivalentsAndMarketableSecurities,quarterlyOtherShortTermInvestments,quarterlyCashAndCashEquivalents',
    'quarterlyFreeCashFlow': 'quarterlyFreeCashFlow,quarterlyCapitalExpenditure,quarterlyOperatingCashFlow,quarterlyEndCashPosition,quarterlyBeginningCashPosition,quarterlyChangeInCashSupplementalAsReported,quarterlyCashFlowFromContinuingFinancingActivities,quarterlyNetOtherFinancingCharges,quarterlyChangeInAccountPayable,quarterlyChangeInInventory,quarterlyChangesInAccountReceivables,quarterlyChangeInWorkingCapital,quarterlyStockBasedCompensation,quarterlyDeferredIncomeTax,quarterlyDepreciationAndAmortization,quarterlyNetIncome'
}

DEFAULT_PARAMS = {
    'lang': 'en-US',
    'region': 'US',
    'padTimeSeries': 'true',
    'merge': 'false',
    'corsDomain': 'finance.yahoo.com'
}


def _valid_period_type(type_):
    assert type_ in ('quarterly', 'annual')


def _yahoo_stock(code):
    if code.startswith('6'):
        return f'{code}.SS'
    else:
        return f'{code}.SZ'


def _default_start(type_):
    """默认开始日期"""
    if 'annual' in type_:
        t = pd.Timestamp.now().normalize() - DateOffset(years=4)
        return BYearEnd().apply(t).normalize()
    else:
        t = pd.Timestamp.now().normalize()
        return QuarterEnd().apply(t) - DateOffset(months=6) - DateOffset(months=3*3)


def _finance_url(type_, code, with_ttm, t1=None, t2=None, fields=None):
    code = _yahoo_stock(code)
    num = 2 if 'annual' in type_ else 1
    url = f'https://query{num}.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{code}'
    # 期间参数：以秒为单位的整数，如`1546214400`
    if t1 is None:
        t1 = _default_start(type_)
        t1 = int(t1.timestamp())
    else:
        t1 = int(pd.Timestamp(t1).normalize().timestamp())
    if t2 is None:
        t2 = int(pd.Timestamp.now().normalize().timestamp())
    else:
        t2 = int(pd.Timestamp(t2).normalize().timestamp())
    params = DEFAULT_PARAMS.copy()
    to_update = {
        'symbol': code,
        'period1': t1,
        'period2': t2,
        'type': TYPE_FIELDS[type_] if with_ttm else TYPE_FIELDS_WITHOUT_TTM[type_]
    }
    params.update(to_update)
    return url, params


def _parse_recordes(x):
    meta = x['meta']
    symbol = meta['symbol'][0]
    item = meta['type'][0]
    item_values = x.get(item, None)
    if item_values is None:
        return []  # [(item, None, None)]
    dates = [pd.Timestamp(d, unit='s') for d in x['timestamp']]
    res = []
    for i in range(len(item_values)):
        item_value = item_values[i]
        if item_value:
            asOfDate = item_value['asOfDate']
            periodType = item_value['periodType']
            value = item_value['reportedValue']['raw']
            res.append((item, asOfDate, value))
        # else:
        #     res.append((item, dates[i], None))
    return res


def _parse_values(json):
    res = []
    for x in json:
        records = _parse_recordes(x)
        res.extend(records)
    cols = ['variable', 'date', 'value']
    df = pd.DataFrame.from_records(res)
    df.columns = cols
    df = df.pivot(index='date', columns='variable', values='value')
    df.columns.name = ''
    df = df.reset_index()
    return df.dropna(subset=['date']).sort_values('date')


def _fetch_report(code, type_, with_ttm):
    url, params = _finance_url(type_, code, with_ttm)
    r = requests.get(url, params=params)
    res = r.json()['timeseries']
    if res['error']:
        raise ConnectionError(res['error'])
    df = _parse_values(res['result'])
    df['symbol'] = code
    return df


def fetch_ebitda(code, with_ttm=False, period_type='quarterly'):
    """最近四期税息折旧及摊销前利润

    Earnings Before Interest, Taxes, Depreciation and Amortization

    Arguments:
        code {str} -- 股票代码

    Keyword Arguments:
        with_ttm {bool} -- 是否包含TTM列 (default: {False})
        period_type {str} -- 周期类型，即年度还是季度 (default: {'quarterly'})
    """
    _valid_period_type(period_type)
    type_ = f"{period_type.lower()}Ebitda"
    df = _fetch_report(code, type_, with_ttm)
    return df


def fetch_total_assets(code, with_ttm=False, period_type='quarterly'):
    """最近四期总资产

    Arguments:
        code {str} -- 股票代码

    Keyword Arguments:
        with_ttm {bool} -- 是否包含TTM列 (default: {False})
        period_type {str} -- 周期类型，即年度还是季度 (default: {'quarterly'})
    """
    _valid_period_type(period_type)
    type_ = f"{period_type.lower()}TotalAssets"
    df = _fetch_report(code, type_, with_ttm)
    return df


def fetch_free_cash_flow(code, with_ttm=False, period_type='quarterly'):
    """最近四期自由现金流

    Arguments:
        code {str} -- 股票代码

    Keyword Arguments:
        with_ttm {bool} -- 是否包含TTM列 (default: {False})
        period_type {str} -- 周期类型，即年度还是季度 (default: {'quarterly'})
    """
    _valid_period_type(period_type)
    type_ = f"{period_type.lower()}FreeCashFlow"
    df = _fetch_report(code, type_, with_ttm)
    return df


def fetch_history(code, start, end):
    """股票期间历史日线数据（含调整收盘价）

    Arguments:
        code {str} -- 股票代码
        start {date like} -- 开始日期
        end {date like} -- 结束日期

    TODO：可能只包含最近一年的数据。废弃！
    """
    t1 = int(pd.Timestamp(start).normalize().timestamp())
    t2 = int(pd.Timestamp(end).normalize().timestamp())
    q_code = _yahoo_stock(code)
    params = {
        'formatted': 'true',
        'crumb': 'oeX65%2F6iJ5F',
        'lang': 'en-US',
        'region': 'US',
        'interval': '1d',
        'period1': t1,
        'period2': t2,
        'events': 'div%7Csplit',
        'corsDomain': 'finance.yahoo.com',
    }
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{q_code}'
    r = requests.get(url, params=params)
    data = r.json()['chart']
    if data['error']:
        raise ConnectionError(data['error'])
    data = data['result'][0]
    dates = [pd.Timestamp(d, unit='s').normalize() for d in data['timestamp']]
    ohlcv = data['indicators']['quote'][0]
    ohlcv = pd.DataFrame.from_dict(ohlcv)
    adjclose = data['indicators']['adjclose'][0]['adjclose']
    ohlcv['date'] = dates
    ohlcv['adjclose'] = adjclose
    ohlcv['symbol'] = code
    return ohlcv
