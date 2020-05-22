import requests
import time
from functools import wraps
import numpy as np
from logbook import Logger

from .._exceptions import ConnectFailed
from ..utils.tools import get_server_name

# 可能会遇到服务器定期重启，导致网络中断。休眠时长应大于重启完成时间
MAX_SLEEP = 2
logger = Logger('休眠')


class DownloadRecord(object):
	called = {}
	start_time = {}
	run_time = {}


def friendly_download(times=20, duration=None, max_sleep=1, show=False):
	"""
	下载函数装饰器

	Parameters
    ----------
	times： int
		每`times`次调用休眠一次
	duration：int
		运行`duration`（秒）休眠一次
	max_sleep：int
		允许最长休眠时间（秒）

	"""
	assert times or duration, '运行次数与时长限制参数不得全部为空'

	def decorator(func):

		key = func.__name__

		def sleep():
			t = np.random.randint(1, max_sleep * 100) / 100
			if show:
				logger.info('每调用函数"{}"{}次，休眠{}秒'.format(key, times, t))
			time.sleep(t)

		@wraps(func)
		def wrapper(*args, **kwargs):

			called = DownloadRecord.called.get(key, 0)
			run_time = DownloadRecord.run_time.get(key, 0)
			start_time = DownloadRecord.start_time.get(key, time.time())

			if times and ((called + 1) % times == 0):
				sleep()
			if duration and (int(run_time + 1) % duration == 0):
				sleep()

			DownloadRecord.called.update({key: called + 1})
			DownloadRecord.run_time.update({key: time.time() - start_time})
			DownloadRecord.start_time.update({key: time.time()})
			return func(*args, **kwargs)
		return wrapper
	return decorator


def _get(url, params, timeout):
    """超时不能设置太短，否则经常出错"""
    for i in range(3):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r
        except requests.exceptions.ConnectionError:
            logger.info('第{}次尝试。无法连接服务器：{}'.format(
            	i + 1, get_server_name(url)))
            time.sleep(MAX_SLEEP)
            continue
        except Exception as e:
            logger.info('第{}次尝试。错误：{}'.format(i + 1, e.args))
        time.sleep(0.1)
    raise ConnectFailed('三次尝试失败。服务器：{}'.format(get_server_name(url)))


def _post(url, params, timeout):
    for i in range(3):
        try:
            r = requests.post(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r
        except requests.exceptions.ConnectionError:
            logger.info('第{}次尝试。无法连接服务器：{}'.format(
            	i + 1, get_server_name(url)))
            time.sleep(MAX_SLEEP)
            continue
        except Exception as e:
            logger.info('第{}次尝试。错误：{}'.format(i + 1, e.args))
        time.sleep(0.1)
    raise ConnectFailed('三次尝试均失败。服务器：{}'.format(get_server_name(url)))


def get_page_response(url, method='get', params=None, timeout=(6, 3)):
    if method == 'get':
        return _get(url, params, timeout)
    else:
        return _post(url, params, timeout)
