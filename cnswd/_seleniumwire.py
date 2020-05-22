import os

from seleniumwire import webdriver
from selenium.webdriver.firefox.options import Options

from .setting.config import DEFAULT_CONFIG
from .utils.path_utils import data_root

LOG_PATH = data_root('geckordriver') / f'{os.getpid()}.log'


def make_headless_browser():
    """无头浏览器"""
    options = Options()
    options.headless = True
    # 禁用gpu加速
    options.add_argument('--disable-gpu')
    return webdriver.Firefox(
        options=options,
        service_log_path=LOG_PATH,
        executable_path=DEFAULT_CONFIG['geckodriver_path'],
        timeout=10)
