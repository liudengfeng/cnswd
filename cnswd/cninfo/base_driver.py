"""
深证信基础模块

菜单层级
+ 每位字符代表一层 842 代表三层菜单
+ 数字代表自然序号，超过10，则以自然序号替代 a=10 b=11
+ 单位层级无子级菜单 如 1
"""
import os
import random
import time
import warnings

import pandas as pd
from retry.api import retry_call
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from toolz.dicttoolz import itemmap

from .._seleniumwire import make_headless_browser
from ..setting.config import POLL_FREQUENCY, TIMEOUT
from ..utils import make_logger
from ..utils.loop_utils import batch_loop, loop_period_by
from .css import CSS
from .ops import (find_menu, find_menu_name_by, find_pos_list,
                  get_all_menu_info, get_current_menu, parse_menu_info,
                  parse_meta_data, read_json_data)

INTERVAL = 0.3

custom_options = {
    'connection_timeout': 20,  # 默认为5s，连接经常出现故障，调整
    'verify_ssl': False,
    # 'connection_keep_alive': False,
    'suppress_connection_errors': True
}

HOME_URL_FMT = 'http://webapi.cninfo.com.cn/#/{}'


class SZXPage(object):
    """深证信基础网页"""

    # 子类需要改写的属性
    api_name = ''
    api_ename = ''
    css = CSS
    delay = 0
    _filter_pattern = {}

    def __init__(self):
        self.driver = None
        self.logger = make_logger(self.api_name)
        self.logger.info("生成无头浏览器")
        self._meta_data = {}
        self._pos = ''

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.driver:
            # self.driver.delete_all_cookies()
            self.driver.quit()

    def __str__(self):
        return f"{self.api_name}{str(os.getpid()).zfill(6)}"

    def ensure_init(self):
        """初始化配置"""
        inited = getattr(self, 'inited', False)
        if inited:
            return
        start = time.time()
        self.driver = make_headless_browser(custom_options)
        self.wait = WebDriverWait(self.driver, TIMEOUT, POLL_FREQUENCY)
        self.logger = make_logger(self.api_name)
        self.driver.get(HOME_URL_FMT.format(self.api_ename))
        self.driver.implicitly_wait(INTERVAL + self.delay)
        m = EC.visibility_of_element_located(
            (By.CSS_SELECTOR, self.css.check_loaded))
        title = self.wait.until(m, message="加载主页失败")
        expected = self.api_name.split('-')[0]
        assert title.text == expected, f"完成加载后，标题应为:{expected}，实际为'{title.text}'。"
        self.logger.info(f'加载耗时：{(time.time() - start):>0.2f}秒')
        # 限定范围，提高性能
        # http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1017?apiname=p_stock2215
        # http://webapi.cninfo.com.cn/api-cloud-platform/apiinfo/info?id=247
        self.driver.scopes = [
            'webapi.cninfo.com.cn',
        ]
        # 首先需要解析启动项目的元数据。确保删除首项元数据请求，保证后续元数据能够正确解析
        start_pos = get_current_menu(self).pos
        self._meta_data[start_pos] = parse_meta_data(self)
        self.inited = True

    @property
    def pos_list(self):
        """网页菜单位置列表"""
        if not getattr(self, '_pos_list', None):
            self.ensure_init()
            self._pos_list = find_pos_list(self, self.css.menu_root)
        return self._pos_list

    @property
    def menu_name_maps(self):
        """菜单名称与菜单位置映射"""
        if not getattr(self, '_menu_name_maps', {}):
            self.ensure_init()
            self._menu_name_maps = {find_menu_name_by(
                self, pos, self.css.menu_root): pos for pos in self.pos_list}
        return self._menu_name_maps

    @property
    def current_menu(self):
        """当前活动菜单"""
        return get_current_menu(self)

    def to_level(self, level):
        """导航至层级菜单

        Args:
            level (str): 指定菜单层级
        """
        if self._pos != level:
            self.ensure_init()
            menu = parse_menu_info(find_menu(self, level))
            self._pos = level
            return menu
        return self.current_menu

    def get_level_meta_data(self, level):
        """项目元数据

        Args:
            level (str): 项目层级

        Returns:
            dict: 元数据字典
        """
        self.ensure_init()
        meta_data = self._meta_data
        level_meta_data = meta_data.get(level, {})
        if not level_meta_data:
            prev_menu = self.current_menu
            self.to_level(level)
            self._meta_data[level] = parse_meta_data(self)
            # 恢复至之前的菜单
            self.to_level(prev_menu.pos)
        return self._meta_data[level]

    def name_to_level(self, name):
        """菜单名称转换为菜单层级"""
        return self.menu_name_maps[name]

    def level_to_name(self, level):
        """菜单层级转换为菜单名称"""
        return itemmap(reversed, self.menu_name_maps)[level]

    def _set_date_filter(self, level, t1, t2):
        pass

    def _before_read(self):
        pass

    def _read_branch_data(self, level, t1, t2):
        """读取分部数据"""
        # 每次读取数据时，重置过滤模式
        self._filter_pattern = {}
        self._set_date_filter(level, t1, t2)
        # 点击查询
        btn_css = self.css.query_btn
        btn = self.driver.find_element_by_css_selector(btn_css)
        # 专题统计中部分项目隐藏命令按钮
        if btn.is_displayed():
            btn.click()
        # 确定发出全部请求
        self._before_read()
        res = read_json_data(self, level)
        return res

    def get_current_period_type(self, level):
        # '子类中重写'
        return ''

    def _internal_ps(self, level, start, end):
        """输出(t1,t2)内部循环元组列表"""
        assert isinstance(start, pd.Timestamp)
        assert isinstance(end, pd.Timestamp)
        loop_str = self.get_current_period_type(level)
        if loop_str is None:
            return [(None, None)]
        freq = loop_str[0]
        fmt_str = loop_str[1]
        tab_id = getattr(self, 'tab_id', 0)
        # 单个股票可以一次性查询期间所有数据
        if tab_id == 1 and fmt_str in ('B', 'D', 'W', 'M'):
            t1, t2 = start.strftime(r'%Y-%m-%d'), end.strftime(r'%Y-%m-%d')
            return [(t1, t2)]
        # 对于财务类，即loop_str为YQ，排除未来日期
        exclude_future = True if loop_str == 'YQ' else False
        ps = loop_period_by(start, end, freq, exclude_future)

        if fmt_str in ('B', 'D', 'W', 'M'):

            def t1_fmt_func(x):
                return x.strftime(r'%Y-%m-%d')

            def t2_fmt_func(x):
                return x.strftime(r'%Y-%m-%d')

        elif fmt_str == 'Q':

            def t1_fmt_func(x):
                return x.year

            def t2_fmt_func(x):
                return x.quarter

        elif fmt_str == 'Y':

            def t1_fmt_func(x):
                return x.year

            def t2_fmt_func(x):
                return None
        else:
            raise ValueError(f'目前不支持FREQ"{loop_str}"格式。')
        return [(t1_fmt_func(p[0]), t2_fmt_func(p[1])) for p in ps]

    def _read_data_by_period(self, level, start, end):
        """划分区间读取数据"""
        res = []
        ps = self._internal_ps(level, start, end)
        name = self.level_to_name(level)
        for t1, t2 in ps:
            data = self._read_branch_data(level, t1, t2)
            self.logger.info(f'{name:<20}  时段 ({t1} ~ {t2}) {len(data):>6}条记录')
            res.extend(data)
        return res

    def reset(self):
        """恢复至初始状态"""
        self.ensure_init()
        # 一般而言出现异常均有提示信息出现
        try:
            self.driver.switch_to.alert.dismiss()
        except Exception:
            pass
        self._filter_pattern = {}
        # 刷新浏览器
        self.driver.refresh()

    def scroll(self, size):
        """
        上下滚动到指定位置

        参数:
        ----
        size: float, 屏幕自上而下的比率
        """
        # 留存
        # 滚动到屏幕底部
        # self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        h = self.driver.get_window_size()['height']
        js = f"var q=document.documentElement.scrollTop={int(size * h)}"
        self.driver.execute_script(js)

    def _is_view(self, elem):
        style = elem.get_attribute('style')
        if 'none' in style:
            return False
        elif 'inline-block' in style:
            return True
