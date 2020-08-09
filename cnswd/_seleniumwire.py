"""
配置driver
    1. 下载Firefox浏览器
    2. 下载最新版本geckodriver，解压后移动到当前用户目录
         https://github.com/mozilla/geckodriver/releases
    3. 将geckodriver.exe所在目录添加path(win)

性能
    1. 初始化一个浏览器大约需要3~4秒

说明
    1. windows 10 edge不支持headless，使用firefox

"""
import logging
import os
from pathlib import Path

from selenium.webdriver.common.proxy import Proxy
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.firefox.options import Options
from seleniumwire import webdriver

from .setting.config import DEFAULT_CONFIG
from .utils.path_utils import data_root

# 压制seleniumwire警告
wire_logger = logging.getLogger('seleniumwire')
wire_logger.setLevel(logging.CRITICAL)


def make_headless_browser(custom_options={}):
    """无头浏览器"""
    # 使用系统代理
    proxy = Proxy()
    proxy.proxy_type = 'SYSTEM'

    fp = FirefoxProfile()

    options = Options()
    # 无头浏览器
    options.headless = True
    # 禁用gpu加速
    options.add_argument('--disable-gpu')
    # 网页加载模式
    # options.page_load_strategy = 'eager'

    default_options = {}
    default_options.update(custom_options)

    log_path = data_root('geckordriver') / f'{os.getpid()}.log'

    return webdriver.Firefox(
        options=options,
        seleniumwire_options=default_options,
        proxy=proxy,
        firefox_profile=fp,
        service_log_path=log_path,
        executable_path=DEFAULT_CONFIG['geckodriver_path'])


def make_headless_browser_with_auto_save_path(download_path, content_type):
    """带自定义下载路径的无头浏览器"""
    options = Options()
    options.headless = True
    # alpha 4 处理路径有误?
    fp = webdriver.FirefoxProfile()
    # 定义下载路径及其他属性
    fp.set_preference("browser.download.folderList", 2)
    fp.set_preference("browser.download.manager.showWhenStarting", False)
    fp.set_preference("browser.download.dir", download_path)
    fp.set_preference("browser.helperApps.neverAsk.saveToDisk", content_type)
    log_path = data_root('geckordriver') / f'{os.getpid()}.log'
    return webdriver.Firefox(
        options=options,
        firefox_profile=fp,
        service_log_path=log_path,
        executable_path=DEFAULT_CONFIG['geckodriver_path'])
