"""
中国国家统计局数据模块
作者：gansaihua
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
from datetime import date
from calendar import monthrange
from .base import get_page_response, friendly_download

HOST_URL = "http://data.stats.gov.cn/easyquery.htm"

QUARTERLY_SUFFIX_MAPS = {1: 'A', 2: 'B', 3: 'C', 4: 'D'}

QUARTERLY_SUFFIX_MAPS_INV = {v: k for k, v in QUARTERLY_SUFFIX_MAPS.items()}


def _extract_date(datestr):
    year = int(datestr[:4])
    month = 12

    if len(datestr) == 5:  # quarterly
        quarter = int(QUARTERLY_SUFFIX_MAPS_INV.get(datestr[4]))
        month = quarter * 3
    elif len(datestr) == 6:  # monthly
        month = int(datestr[4:6])

    day = monthrange(year, month)[1]
    return date(year=year, month=month, day=day)


def _sanitize_date(datestr, freq):
    dt = pd.Timestamp(datestr)

    freq = freq.strip().lower()
    ret = dt.year

    if freq == 'quarterly':
        ret = '%d%s' % (ret, QUARTERLY_SUFFIX_MAPS.get(dt.quarter))
    elif freq == 'monthly':
        ret = '%d%s' % (ret, str(dt.day).zfill(2))

    return ret


def _freq_to_dbcode(freq):
    ref = {
        'monthly': 'hgyd',
        'quarterly': 'hgjd',
        'yearly': 'hgnd',
    }
    return ref.get(freq.strip().lower())


@friendly_download(times=66, duration=None, max_sleep=1)
def fetch_economics(code, start, end, freq):
    '''freq = monthly, quarterly, yearly'''
    start = _sanitize_date(start, freq)
    end = _sanitize_date(end, freq)

    date_rng = start + '-' + end

    params = {
        'm':
        'QueryData',
        'rowcode':
        'zb',
        'colcode':
        'sj',
        'wds':
        '[]',
        'dbcode':
        _freq_to_dbcode(freq),
        'dfwds':
        '[{"wdcode":"zb","valuecode":"%s"}, {"wdcode":"sj","valuecode": "%s"}]'
        % (code, date_rng),
    }

    r = get_page_response(HOST_URL, method='post', params=params)

    records = []
    labels = ['code', 'asof_date', 'value']
    for record in r.json()['returndata']['datanodes']:
        val = record['data']
        if val['hasdata']:
            code = record['wds'][0]['valuecode']
            asof_date = record['wds'][1]['valuecode']
            records.append((code, _extract_date(asof_date), val['data']))

    df = pd.DataFrame.from_records(records, columns=labels)
    return df


def get_codes(freq, node_id='zb'):
    '''freq = monthly, quarterly, yearly
   public API
   '''
    return _batch_leaf_codes(freq, node_id)


def get_categories(freq, node_id='zb'):
    ''' return the categories which are parents 
   or super-parents codes of series
   node_id should be nodes which are super-parents not direct parents of leafs
   '''
    return _batch_page_codes(freq, node_id)[0]


def _batch_leaf_codes(freq, node_id='zb'):
    '''return all the codes of series which are children to the node of node_id
   default the root node'''
    ret = []

    page_codes = _batch_page_codes(freq, node_id)[1]

    if page_codes.empty:
        page_codes = [node_id]
    else:
        page_codes = page_codes['id']

    for page_code in page_codes:
        res = _get_leaf_codes(freq, page_code)
        ret.append(res)

    if ret:
        ret = pd.concat(ret)
    else:
        ret = pd.DataFrame()
    return ret


@friendly_download(times=66, duration=None, max_sleep=1)
def _get_leaf_codes(freq, page_code):
    '''return list of code which directly denotes a series
   page_code should be the node which are direct parent to leafs'''
    params = {
        'm': 'QueryData',
        'rowcode': 'zb',
        'colcode': 'sj',
        'wds': '[]',
        'dbcode': _freq_to_dbcode(freq),
        'dfwds': '[{"wdcode":"zb","valuecode":"%s"}]' % page_code,
    }

    r = get_page_response(HOST_URL, method='post', params=params)

    res = r.json()

    records = []
    for cval in res['returndata']['wdnodes'][0]['nodes']:
        code = cval['code']
        cname = cval['cname']
        unit = cval['unit']
        row = (code, cname, unit)
        records.append(row)

    labels = ['code', 'cname', 'unit']
    df = pd.DataFrame.from_records(records, columns=labels)

    return df


def _batch_page_codes(freq, node_id='zb'):
    '''freq = monthly, quarterly, yearly
   return all the final tree nodes which can be scrolled
   '''
    nodes = []
    parents_of_leafs = []

    queue = _get_page_codes(freq, node_id)
    while not queue.empty:
        is_parent = queue['isParent']

        parents_of_leafs.append(queue[~is_parent])

        queue = queue[is_parent]
        nodes.append(queue.copy())

        ids = queue['id']
        for nid in ids:
            row_to_remove = (queue['id'] == nid)
            queue = queue[~row_to_remove]

            queue = queue.append(_get_page_codes(node_id=nid, freq=freq),
                                 ignore_index=True)
    if nodes:
        nodes = pd.concat(nodes)
    else:
        nodes = pd.DataFrame()

    if parents_of_leafs:
        parents_of_leafs = pd.concat(parents_of_leafs)
    else:
        parents_of_leafs = pd.DataFrame()

    nodes = pd.concat([nodes, parents_of_leafs], ignore_index=True)

    return (nodes, parents_of_leafs)


@friendly_download(times=33, duration=None, max_sleep=1)
def _get_page_codes(freq='quarterly', node_id='zb'):
    '''default: the children of the root
   return the direct children to the node_id'''
    params = {
        'id': node_id,
        'dbcode': _freq_to_dbcode(freq),
        'wdcode': 'zb',
        'm': 'getTree',
    }

    r = get_page_response(HOST_URL, method='post', params=params)

    return pd.DataFrame.from_records(r.json())
