
from cnswd.mongodb import get_db
import json
from googletrans import Translator
import regex


W_PAT = regex.compile(
    r'([a-z]{1,})|([A-Z][a-z]{1,})|([A-Z]{2,})([A-Z][a-z]{1,})|([A-Z]{2,}$)')


def _split(ws):
    return ' '.join([x for item in W_PAT.findall(ws) for x in item if len(x)])

# 验证脚本
# l = ['NetIncomeContinuousOperations', 'InterestIncome', 'DilutedEPS', 'NetIncomeIncludingNoncontrollingInterests', 'CostOfRevenue', 'NormalizedEBITDA', 'TotalRevenue', 'EBIT', 'PretaxIncome', 'OtherNonOperatingIncomeExpenses', 'GeneralAndAdministrativeExpense', 'symbol', 'TotalOperatingIncomeAsReported', 'ResearchAndDevelopment', 'NetIncomeFromContinuingAndDiscontinuedOperation', 'OperatingIncome', 'TaxEffectOfUnusualItems', 'SellingAndMarketingExpense', 'NetIncomeCommonStockholders', 'NetNonOperatingInterestIncomeExpense', 'TaxProvision', 'SpecialIncomeCharges', 'DilutedAverageShares', 'OperatingRevenue', 'ReconciledDepreciation',
#      'GrossProfit', 'TaxRateForCalcs', 'EBITDA', 'BasicEPS', 'OtherOperatingExpenses', 'NetInterestIncome', 'TotalUnusualItems', 'BasicAverageShares', 'ReconciledCostOfRevenue', 'NormalizedIncome', 'OtherSpecialCharges', 'NetIncomeFromContinuingOperationNetMinorityInterest', 'TotalUnusualItemsExcludingGoodwill', 'TotalExpenses', 'InterestExpense', 'periodType', 'NetIncome', 'InterestExpenseNonOperating', 'TotalOtherFinanceCost', 'InterestIncomeNonOperating', 'WriteOff', 'GainOnSaleOfSecurity', 'asOfDate', 'MinorityInterests', 'OperatingExpense', 'OtherunderPreferredStockDividend', 'SellingGeneralAndAdministration', 'ImpairmentOfCapitalAssets']

# for ws in l:
#     print(ws, '->', _split(ws))


# translator = Translator(service_urls=[
#     'translate.google.cn',
# ])
# text = _split(ws)
# translation = translator.translate(text, src='en', dest='zh-cn')

# print(translation.origin, ' -> ', translation.text)


def to_cn(word_list):
    assert isinstance(word_list, list)
    translator = Translator(service_urls=[
        'translate.google.cn',
    ])
    # print(word_list)
    text = [_split(ws) for ws in word_list]
    # print(text)
    ts = translator.translate(text, src='en', dest='zh-cn')
    return {''.join(t.origin.split()): t.text for t in ts}


def get_items(db, tname):
    collection = db[tname]
    items = collection.find(projection={'_id': 0})
    res = []
    for d in items:
        res.extend(list(d.keys()))
    return set(res)


def get_table_name_maps(tname):
    db = get_db('yahoo')
    items = list(get_items(db, tname))
    print(tname)
    # print("="*80)
    return to_cn(items)


if __name__ == '__main__':
    db = get_db('yahoo')
    for tname in db.list_collection_names():
        with open(f'{tname}.json', 'w', encoding='utf8') as f:
            data = get_table_name_maps(tname)
            json.dump(data, f)
