"""

深证信专题统计模块

作为数据提取工具，专题统计的大部分项目没有实际意义，只是对数据进行的统计加工。
有效栏目如`股票状态`等数据搜索未出现的部分

备注：
    当前只用到融资融券(8.2)，其余部分并未测试。
"""
import json
import os
import random
import re
import time

import numpy as np
import pandas as pd
from selenium.common.exceptions import (ElementNotInteractableException,
                                        NoSuchElementException,
                                        StaleElementReferenceException,
                                        TimeoutException)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from .._exceptions import RetryException, FutureDate
from .._seleniumwire import make_headless_browser
from ..setting.config import POLL_FREQUENCY, TIMEOUT, TS_CONFIG
from ..utils import make_logger
from ..utils.loop_utils import loop_codes, loop_period_by
from ..utils.pd_utils import _concat
from .ops import (
    change_year, datepicker, navigate, toggler_open, wait_for_activate,
    wait_for_all_presence, wait_for_invisibility, wait_for_preview,
    wait_for_visibility, wait_page_loaded)
from .utils import get_field_map


class ThematicStatistics(object):
    """深证信专题统计api"""

    # 类公用变量
    config = TS_CONFIG
    api_name = '专题统计'

    # 以此元素是否显示为标准，检查页面是否正确加载
    check_loaded_css = '.nav-second > div:nth-child(1) > h1:nth-child(1)'
    check_loaded_css_value = '专题统计'
    data_loaded_css = ''

    # 改写的属性
    preview_btn_css = 'button.stock-search:nth-child(11)'

    wait_for_preview_css = '.fixed-table-loading'  # '.fixed-table-header'
    level_input_css = '.api-search-left > input:nth-child(1)'
    level_query_bnt_css = '.api-search-left > i:nth-child(2)'

    # 以此元素是否显示为标准，检查页面是否正确加载
    check_loaded_css = 'div.ul-container:nth-child(2) > ul:nth-child(1) > li:nth-child(1) > a:nth-child(1)'

    def __init__(self, log_to_file=None):
        url = 'http://webapi.cninfo.com.cn/#/thematicStatistics'
        start = time.time()
        self.log_to_file = log_to_file
        self.driver = make_headless_browser()
        self.wait = WebDriverWait(self.driver, TIMEOUT, POLL_FREQUENCY)
        name = f"{self.api_name}{str(os.getpid()).zfill(6)}"
        self.logger = make_logger(name, log_to_file)
        self.driver.get(url)
        # 首次加载耗时
        self.driver.implicitly_wait(1)
        # 确保加载完成
        self._wait_for_preview()
        # # 特定元素可见，完成首次页面加载
        # wait_page_loaded(self.wait, self.check_loaded_css,
        #                  self.check_loaded_css_value, msg)
        self.logger.notice(f'加载主页用时：{(time.time() - start):>0.4f}秒')

        # 类变量
        self.code_loaded = False
        self.current_level = ''
        self.code_loaded = False
        self.current_t1_css = ''
        self.current_t2_css = ''
        self.current_t1_value = ''
        self.current_t2_value = ''
        self.current_code = ''

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.driver.quit()

    def reset(self):
        """恢复至初始状态"""
        self.driver.refresh()

        msg = f"首次加载{self.api_name}超时"
        # 特定元素可见，完成首次页面加载
        wait_page_loaded(self.wait, self.check_loaded_css,
                         self.check_loaded_css_value, msg)

        # 类变量
        self.code_loaded = False
        self.current_level = ''
        self.code_loaded = False
        self.current_t1_css = ''
        self.current_t2_css = ''
        self.current_t1_value = ''
        self.current_t2_value = ''
        self.current_code = ''

    def select_nav(self, level):
        if self.current_level != level:
            navigate(self.driver, level)
            self.current_level = level
            # 转换需要等待
            self.driver.implicitly_wait(1)
            # 删除
            del self.driver.requests

    def set_t1_value(self, t1):
        """更改查询t1值"""
        self.current_t1_css = self.config[self.current_level]['css'][0]
        if self.current_t1_css and (t1 != self.current_t1_value):
            # self.scroll(0.7)
            # 输入日期字符串时
            if 'input' in self.current_t1_css:
                datepicker(self.driver, self.current_t1_css, t1)
                self.current_t1_value = t1
            elif self.current_t1_css in ('#se1_sele', '#se2_sele'):
                change_year(self.driver, self.current_t1_css, t1)
                self.current_t1_value = t1
            # self.driver.save_screenshot('t1.png')

    def set_t2_value(self, t2):
        """更改查询t2值"""
        self.current_t2_css = self.config[self.current_level]['css'][1]
        if self.current_t2_css and (t2 != self.current_t2_value):
            # self.scroll(0.7)
            if 'input' in self.current_t2_css:
                datepicker(self.driver, self.current_t2_css, t2)
                self.current_t2_value = t2
            elif self.current_t2_css.startswith('.condition2'):
                elem = self.driver.find_element_by_css_selector(
                    self.current_t2_css)
                t2 = int(t2)
                assert t2 in (1, 2, 3, 4), '季度有效值为(1,2,3,4)'
                select = Select(elem)
                select.select_by_index(t2 - 1)
                self.current_t2_value = t2
            # self.driver.save_screenshot('t2.png')

    def _before_read(self):
        # 预览数据
        # 专题统计中，部分项目无命令按钮
        if any(self.config[self.current_level]['css']):
            # 预览数据
            self.driver.find_element_by_css_selector(
                self.preview_btn_css).click()
        else:
            # 没有预览按钮时，等待一小段时间
            self.driver.implicitly_wait(0.1)
        self._wait_for_preview()
        # 故当前一次数据为空时，后续继续判断为空
        # 需要强制等待完成加载
        time.sleep(0.3)

    def _get_data(self, level, t1, t2):
        """读取项目数据"""
        self.set_t1_value(t1)
        self.set_t2_value(t2)
        return self._loop_options(level)

    def _loop_by_period(self, level, start, end):
        # 循环指示字符
        loop_str = self.config[level]['date_freq'][0]
        # 排除选项
        include = self.config[level]['date_freq'][1]
        if loop_str is None:
            return self._get_data(level, None, None)
        freq = loop_str[0]
        fmt_str = loop_str[1]
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
            raise ValueError(f'{loop_str}为错误格式。')
        ps = loop_period_by(start, end, freq, include)
        res = []
        for i, (s, e) in enumerate(ps, 1):
            t1, t2 = t1_fmt_func(s), t2_fmt_func(e)
            self._log_info('>', level, t1, t2)
            data = self._get_data(level, t1, t2)
            res.extend(data)
            if i % 10 == 0:
                time.sleep(np.random.random())
        return res

    def _view_message(self, p, level, start, end, s=''):
        """构造显示信息"""
        width = 30
        if level is None:
            item = ''
        else:
            item = self.config[level]['name']
        if pd.api.types.is_number(start):
            if pd.api.types.is_number(end):
                msg = f'{start}年{end}季度'
            else:
                if end:
                    msg = f'{start}年 ~ {end}'
                else:
                    msg = f'{start}年'
        else:
            if start:
                msg = pd.Timestamp(start).strftime(r'%Y-%m-%d')
                if end:
                    msg += f" ~ {pd.Timestamp(end).strftime(r'%Y-%m-%d')}"
                else:
                    msg = f"{pd.Timestamp(start).strftime(r'%Y-%m-%d')} ~ {pd.Timestamp('today').strftime(r'%Y-%m-%d')}"
            else:
                msg = ''
        left = f"{p}{item}"
        return f"{left:{width}} {msg} {s}"

    def _log_info(self, p, level, start, end, s=''):
        self.logger.info(self._view_message(p, level, start, end, s))

    def _try_get_data(self, level, start=None, end=None):
        self.select_nav(level)
        if self.current_t1_css:
            # 由于专题统计会预先加载默认数据，需要等待日期元素可见后，才可执行下一步
            wait_for_visibility(self.driver, self.current_t1_css,
                                self.api_name)
        data = self._loop_by_period(level, start, end)
        data = pd.DataFrame.from_dict(data)
        columns_mapper = get_field_map('ts', level)
        data = pd.DataFrame.from_dict(data)
        # 不存在没有列映射表时，返回原始表
        if len(columns_mapper) == 0:
            return data
        df = pd.DataFrame()
        if not data.empty:
            # 保持列原始顺序
            for k, v in columns_mapper.items():
                df[v] = data[k]
        return df

    def get_data(self, level, start=None, end=None):
        """获取项目数据

        Arguments:
            level {str} -- 项目层级

        Keyword Arguments:
            start {str} -- 开始日期 (default: {None})，如为空，使用市场开始日期
            end {str} -- 结束日期 (default: {None})，如为空，使用当前日期


        Usage:
            >>> api = WebApi()
            >>> api.get_data('4.1','2018-01-01','2018-08-01')

        Returns:
            pd.DataFrame -- 如期间没有数据，返回长度为0的空表
        """
        for i in range(1, 6):
            try:
                return self._try_get_data(level, start, end)
            except FutureDate:
                return pd.DataFrame()
            except Exception as e:
                self.reset()
                self.logger.notice(f"第{i}次尝试\n\n {e!r}")
                time.sleep(random.random())
        raise RetryException(f"尝试失败")

    def _get_target_paths(self):
        paths = []
        api_key = self.config[self.current_level]['api_key']
        for r in self.driver.requests:
            if r.method == 'POST' and api_key in r.path:
                paths.append(r.path)
        return paths

    def _read_json_data(self):
        self._before_read()
        res = []
        paths = self._get_target_paths()
        for p in set(paths):
            # 加大超时时长
            r = self.driver.wait_for_request(p, timeout=15)
            try:
                data = json.loads(r.response.body)
                res.extend(data['records'])
            except Exception as e:
                self.logger.info(e)
        # 删除已经读取的请求
        del self.driver.requests
        return res

    def _loop_options(self, level):
        """循环读取所有可选项目数据"""
        # 第三项为选项css
        opt_css = self.config[level]['css'][2]
        if opt_css is None:
            return self._read_json_data()
        label_css = opt_css.split('>')[0] + ' > label:nth-child(1)'
        label = self.driver.find_element_by_css_selector(label_css)
        if label.text in ('交易市场', '控制类型'):
            return self._read_all_option(opt_css)
        else:
            return self._read_one_by_one(opt_css)

    def _read_one_by_one(self, css):
        """逐项读取选项数据"""
        res = []
        elem = self.driver.find_element_by_css_selector(css)
        select = Select(elem)
        options = select.options
        for o in options:
            self.logger.info(f'{o.text}')
            select.select_by_visible_text(o.text)
            data = self._read_json_data()
            res.extend(data)
        return res

    def _read_all_option(self, css):
        """读取`全部`选项数据"""
        elem = self.driver.find_element_by_css_selector(css)
        select = Select(elem)
        select.select_by_value("")
        return self._read_json_data()

    @property
    def is_available(self):
        """
        故障概率低，返回True
        """
        return True

    def _wait_for_preview(self, style=None):
        """等待预览结果完全呈现"""
        # 与数据搜索有差异
        css = '.fixed-table-loading'
        wait_for_invisibility(self.driver, css)
