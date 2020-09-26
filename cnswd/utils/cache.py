"""
当查询、计算耗时较长的数据时，使用缓存
"""
from hashlib import blake2b

import pandas as pd

from .path_utils import data_root

CACHE_DIR_NAME = 'hotdata'
DIGEST_SIZE = 10


class HotDataCache(object):
    """缓存Series、DataFrame数据"""

    def __init__(self, func, delta='1D', hour=0, minute=0, *args, **kwargs):
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self.delta = delta
        self.hour = hour
        self.minute = minute

    def fetch_data(self):
        return self._func(*self._args, **self._kwargs)

    @property
    def table_name(self):
        p1 = self._func.__name__
        p2 = ''.join([str(x) for x in self._args])
        p3 = ''.join([str(x) for x in self._kwargs.values()])
        s = p1 + p2 + p3
        h = blake2b(digest_size=DIGEST_SIZE)
        h.update(s.encode())
        return h.hexdigest()

    def _gen_data(self):
        data = self.fetch_data()
        return {
            'records': data,
            'refresh_time': pd.Series({'refresh_time': pd.Timestamp.now()}),
        }

    @ property
    def store_path(self):
        return data_root(f"{CACHE_DIR_NAME}/{self.table_name}.h5")

    def _insert(self):
        with pd.HDFStore(str(self.store_path)) as store:
            doc = self._gen_data()
            for k, v in doc.items():
                store.put(k, v)

    def _update(self):
        with pd.HDFStore(str(self.store_path)) as store:
            try:
                store.remove('records')
                store.remove('refresh_time')
            except KeyError:
                pass
        self._insert()

    @ property
    def last_refresh_time(self):
        with pd.HDFStore(str(self.store_path)) as store:
            try:
                dt = store.select('refresh_time')['refresh_time']
                store.close()
                return dt
            except Exception:
                return pd.Timestamp('1970-01-01')

    @ property
    def next_refresh_time(self):
        last = self.last_refresh_time
        next_time = last + pd.Timedelta(self.delta)
        next_time = pd.Timestamp(next_time)
        next_time = next_time.floor('T')
        return next_time.replace(hour=self.hour, minute=self.minute)

    @ property
    def data(self):
        now = pd.Timestamp.now()
        if now >= self.next_refresh_time:
            self._update()
        with pd.HDFStore(str(self.store_path)) as store:
            df = store.select('records')
            return df


def clean_cache():
    """清除缓存数据"""
    root = data_root(f"{CACHE_DIR_NAME}")
    for path in root.glob('*.h5'):
        if len(path.name.split('.')[0]) == 2 * DIGEST_SIZE:
            path.unlink()
