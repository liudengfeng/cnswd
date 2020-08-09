"""

深证信基础模块

"""
import os
import random
import time
import warnings

import pandas as pd
from retry.api import retry_call
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.common.exceptions import TimeoutException

from .._seleniumwire import make_headless_browser
from ..setting.config import POLL_FREQUENCY, TIMEOUT
from ..utils import make_logger
from ..utils.loop_utils import batch_loop, loop_period_by
from .ops import (datepicker, get_nav_info, input_code,
                  element_attribute_change_to, parse_response, find_by_id,
                  element_text_change_to, navigate, parse_meta_data,
                  read_json_data, simulated_click, select_year, select_quarter)

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
    query_btn_css = ''
    delay = 0

    def __init__(self):
        self.driver = None
        self.logger = make_logger(self.api_name)
        self.logger.info("生成无头浏览器")
        self._meta_data = {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.driver:
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
        check_css = '.nav-title'
        m = EC.visibility_of_element_located((By.CSS_SELECTOR, check_css))
        title = self.wait.until(m, message="加载主页失败")
        expected = self.api_name.split('-')[0]
        assert title.text == expected, f"完成加载后，标题应为:{expected}，实际为'{title.text}'。"
        # self.driver.save_screenshot('p.png')
        self.logger.info(f'加载耗时：{(time.time() - start):>0.2f}秒')
        # 限定范围，提高性能
        # http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1017?apiname=p_stock2215
        # http://webapi.cninfo.com.cn/api-cloud-platform/apiinfo/info?id=247
        self.driver.scopes = [
            'webapi.cninfo.com.cn',
        ]
        self.inited = True

    @property
    def levels(self):
        """数据层级信息"""
        self.ensure_init()
        _levels = getattr(self, '_levels', None)
        if not _levels:
            self.logger.info("提取项目元数据信息......")
            self._levels = get_nav_info(self)
        return self._levels

    def name_to_level(self, name):
        for info in self.levels:
            if info['名称'] == name:
                return info['层级']
        raise ValueError(f"不存在数据项目:{name}")

    def get_level_meta_data(self, level):
        """项目元数据

        Args:
            level (str): 项目层级

        Returns:
            dict: 元数据字典
        """
        _meta_data = getattr(self, '_meta_data', {})
        level_meta_data = _meta_data.get(level, {})
        if not level_meta_data:
            self.ensure_init()
            self.to_level(level)
            self._meta_data[level] = parse_meta_data(self)
        return self._meta_data[level]

    def to_level(self, level):
        """导航至层级菜单

        Args:
            level (str): 指定菜单层级
        """
        # 默认值必须设置为空
        current_level = getattr(self, 'current_level', '')
        if current_level != level:
            navigate(self, level)
            self.current_level = level
            self.driver.implicitly_wait(INTERVAL)

    def _read_branch_data(self, t1, t2):
        """读取分部数据"""
        self._set_date_filter(t1, t2)
        # 点击查询
        btn_css = self.query_btn_css
        btn = self.driver.find_element_by_css_selector(btn_css)
        # 专题统计中部分项目隐藏命令按钮
        if btn.is_displayed():
            btn.click()
        # 等待完全发送查询指令
        retry_call(self._before_read,
                   exceptions=(TimeoutException, ),
                   tries=3,
                   logger=self.logger)
        res = read_json_data(self)
        return res

    def _internal_ps(self, start, end):
        """输出(t1,t2)内部循环元组列表"""
        assert isinstance(start, pd.Timestamp)
        assert isinstance(end, pd.Timestamp)
        loop_str = self._current_period_type()
        if loop_str is None:
            return [(None, None)]
        freq = loop_str[0]
        fmt_str = loop_str[1]
        tab_id = getattr(self, 'tab_id', '')
        # 单个股票可以一次性查询期间所有数据
        if tab_id == 1 and fmt_str in ('B', 'D', 'W', 'M'):
            t1, t2 = start.strftime(r'%Y-%m-%d'), end.strftime(r'%Y-%m-%d')
            return [(t1, t2)]
        # 对于财务类，即周期频率为Q，排除未来日期
        ps = loop_period_by(start, end, freq, False) if freq != 'Q' else loop_period_by(
            start, end, freq, True)

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

    def _read_data_by_period(self, start, end):
        """划分区间读取数据"""
        res = []
        ps = self._internal_ps(start, end)
        for t1, t2 in ps:
            self.logger.info(f'{self.current_item:<20}  时段 ({t1} ~ {t2})')
            data = self._read_branch_data(t1, t2)
            res.extend(data)
        return res

    def reset(self):
        """恢复至初始状态"""
        self._ensure_init()
        # 一般而言出现异常均有提示信息出现
        try:
            self.driver.switch_to.alert.dismiss()
        except Exception:
            pass
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
