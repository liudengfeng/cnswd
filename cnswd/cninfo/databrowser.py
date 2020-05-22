import json
import os
import random
import re
import time

import pandas as pd
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from .._exceptions import FutureDate, RetryException
from .._seleniumwire import make_headless_browser
from ..setting.config import DB_CONFIG, POLL_FREQUENCY, TIMEOUT
from ..utils import ensure_list, make_logger, sanitize_dates
from ..utils.loop_utils import loop_codes, loop_period_by
from ..utils.pd_utils import _concat
from .ops import (change_year, datepicker, navigate, toggler_open,
                  wait_for_activate, wait_for_all_presence, wait_for_preview,
                  wait_for_visibility, wait_page_loaded)
from .utils import get_field_map


class DataBrowser(object):
    """深证信数据浏览器基础类"""
    # 类公用变量
    api_name = ''
    config = DB_CONFIG
    # 以此元素是否显示为标准，检查页面是否正确加载
    check_loaded_css = '.nav-second > div:nth-child(1) > h1:nth-child(1)'
    check_loaded_css_value = api_name

    def __init__(self, log_to_file=None):
        self.log_to_file = log_to_file
        self.driver = None
        self.init_driver = False
        name = f"{self.api_name}{str(os.getpid()).zfill(6)}"
        self.logger = make_logger(name, self.log_to_file)

    def _ensure_init(self):
        url = 'http://webapi.cninfo.com.cn/#/dataBrowse'
        start = time.time()
        self.driver = make_headless_browser()
        self.wait = WebDriverWait(self.driver, TIMEOUT, POLL_FREQUENCY)
        name = f"{self.api_name}{str(os.getpid()).zfill(6)}"
        self.logger = make_logger(name, self.log_to_file)
        self.driver.get(url)
        # 确保加载完成
        msg = f"首次加载{self.api_name}超时"
        # 特定元素可见，完成首次页面加载
        wait_page_loaded(self.wait, self.check_loaded_css,
                         self.check_loaded_css_value, msg)
        self.driver.implicitly_wait(1.5)
        self.logger.notice(f'加载主页用时：{(time.time() - start):>0.4f}秒')
        self._base_config()
        self.init_driver = True

    def _base_config(self):
        # 类变量
        self.code_loaded = False
        self.current_level = ''
        self.current_t1_css = ''
        self.current_t2_css = ''
        self.current_t1_value = ''
        self.current_t2_value = ''
        self.current_code = ''

        # 转换模式
        self._bt()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.driver:
            self.driver.quit()

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
        # self.driver.implicitly_wait(0.5)
        time.sleep(1.5)
        # 恢复基础配置
        self._base_config()

    def _bt(self):
        raise NotImplementedError('子类中完成')

    def _view_message(self, p, level, start, end, s=''):
        """构造显示信息"""
        width = 30
        if level is None:
            item = ''
        else:
            item = f"{self.config[level]['name']}({level})"
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

    def select_nav(self, level):
        if self.current_level != level:
            navigate(self.driver, level)
            self.current_level = level

    def _load_all_code(self, codes):
        """选择查询的股票代码"""
        pass

    def _before_read(self):
        pass

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

    def _get_target_paths(self):
        paths = []
        api_key = self.config[self.current_level]['api_key']
        for r in self.driver.requests:
            if r.method == 'POST' and api_key in r.path:
                paths.append(r.path)
        return paths

    def _read_json_data(self):
        res = []
        paths = self._get_target_paths()
        for p in sorted(paths):
            # s = time.time()
            # 加大超时时长
            r = self.driver.wait_for_request(p, timeout=90)
            # print(f'{p} 等待时长:{time.time() - s:0.4f}')
            # print('状态码', r.response.status_code)
            data = json.loads(r.response.body)
            res.extend(data['records'])
            # print(f"行数 {data['total']}")
        # 删除已经读取的请求
        del self.driver.requests
        return res

    def _get_data(self, t1, t2):
        """读取项目数据"""
        self.set_t1_value(t1)
        self.set_t2_value(t2)
        # 点击预览按钮
        btn = '.stock-search'
        self.driver.find_element_by_css_selector(btn).click()
        self._before_read()
        return self._read_json_data()

    def get_loop_period(self, level, start, end):
        loop_str = self.config[level]['date_freq'][0]
        include = self.config[level]['date_freq'][1]
        # 第一个字符指示循环周期freq
        freq = loop_str[0]
        return loop_period_by(start, end, freq, include)

    def get_data(self, level, start=None, end=None):
        raise NotImplementedError('子类中完成')

    def click_elem(self, elem):
        """点击所选元素"""
        self.driver.execute_script("arguments[0].scrollIntoView();", elem)
        actions = ActionChains(self.driver)
        actions.move_to_element(elem).click().perform()

    def scroll(self, size):
        """
        上下滚动到指定位置

        参数:
        ----
        size: float, 屏幕自上而下的比率
        """
        # 滚动到屏幕底部
        # self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        h = self.driver.get_window_size()['height']
        js = f"var q=document.documentElement.scrollTop={int(size * h)}"
        self.driver.execute_script(js)


class FastSearcher(DataBrowser):
    """
    快速搜索

    备注：
        单只股票期间循环方式不同于高级搜索
    """
    api_name = '快速搜索'
    input_code_css = '#input_code'

    def _bt(self):
        try:
            self.driver.find_element_by_id('btn1').click()
        except Exception:
            pass

    def _select_code(self, code):
        """输入代码"""
        elem = self.driver.find_element_by_css_selector(self.input_code_css)
        elem.clear()
        elem.send_keys(code)
        # 选中第一项
        searched_css = 'div.searchDataRes:nth-child(2) > p:nth-child(1)'
        wait_for_visibility(self.driver, searched_css)
        self.driver.find_element_by_css_selector(searched_css).click()
        self.current_code = code

    def _loop_by_period(self, level, start, end):
        """分时期段读取数据"""
        loop_str = self.config[level]['date_freq'][0]
        if loop_str is None:
            return self._get_data(None, None)
        # 第二个字符指示值的表达格式
        fmt_str = loop_str[1]
        if fmt_str in ('B', 'D', 'W', 'M'):
            # 单个股票期间数据量小
            # 对于单个股票期间数据无需循环，直接设置期间即可
            start = pd.Timestamp(start)
            end = pd.Timestamp(end)
            t1, t2 = start.strftime(r'%Y-%m-%d'), end.strftime(r'%Y-%m-%d')
            data = self._get_data(t1, t2)
            return data

        ps = self.get_loop_period(level, start, end)
        if fmt_str == 'Q':

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
        res = []
        for s, e in ps:
            t1, t2 = t1_fmt_func(s), t2_fmt_func(e)
            self.logger.info(f'>  时段 {t1} ~ {t2}')
            data = self._get_data(t1, t2)
            res.extend(data)
        return res

    def _try_get_data(self, level, code, start=None, end=None):
        start, end = sanitize_dates(start, end)
        if not self.init_driver:
            self._ensure_init()
        self._bt()
        self.select_nav(level)
        assert re.compile(r'^\d{6}$').match(
            code), f'`code`参数应为6位数字的股票代码，实际为：{code}'
        self._select_code(code)
        self._log_info(self.current_code, level, start, end)
        data = self._loop_by_period(level, start, end)
        columns_mapper = get_field_map('db', level)
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

    def get_data(self, level, code, start=None, end=None):
        """获取项目数据

        Arguments:
            level {str} -- 项目层级
            code {str} -- 股票代码

        Keyword Arguments:
            start {str} -- 开始日期 (default: {None})，如为空，使用市场开始日期
            end {str} -- 结束日期 (default: {None})，如为空，使用当前日期


        Usage:
            >>> api = FastSearch()
            >>> api.get_data('2.1','000333', '2018-01-01','2018-08-10')
            >>> api.driver.quit()

        Returns:
            pd.DataFrame -- 如期间没有数据，返回长度为0的空表
        """
        for i in range(1, 6):
            try:
                return self._try_get_data(level, code, start, end)
            except FutureDate:
                return pd.DataFrame()
            except Exception as e:
                # 删除请求
                del self.driver.requests
                self.reset()
                self.logger.notice(f"第{i}次尝试\n\n {e!r}")
                time.sleep(random.random())
        raise RetryException(f"尝试失败")


class AdvanceSearcher(DataBrowser):
    """高级搜索"""
    api_name = '高级搜索'
    _codes = None

    def _bt(self):
        try:
            self.driver.find_element_by_id('btn2').click()
        except Exception:
            pass

    def _select_all_fields(self):
        """全选字段"""
        # 使用i元素
        label_css = '.detail-cont-bottom > div:nth-child(1) > div:nth-child(1) > label:nth-child(1)'
        btn_css = '.detail-cont-bottom > div:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
        # 全选数据字段
        self._add_or_delete_all(label_css, btn_css)

    def _load_all_code(self):
        """选择查询的股票代码"""
        if self.code_loaded:
            return
        markets = ['深市A', '深市B', '中小板', '创业板', '沪市A', '沪市B', '科创板']
        market_cate_css = '.classify-tree > li:nth-child(6)'
        wait_for_visibility(self.driver, market_cate_css)
        li = self.driver.find_element_by_css_selector(market_cate_css)
        toggler_open(li)
        xpath_fmt = "//a[@data-name='{}']"
        to_select_css = '.cont-top-right > div:nth-child(1) > div:nth-child(3) > ul:nth-child(1) li'
        add_label_css = '.cont-top-right > div:nth-child(1) > div:nth-child(1) > label:nth-child(1)'
        add_btn_css = '.cont-top-right > div:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
        for market in markets:
            self.logger.info(f"加载{market}代码")
            # 选择市场分类
            self.driver.find_element_by_xpath(xpath_fmt.format(market)).click()
            wait_for_activate(self.driver, market)
            # 等待加载代码
            wait_for_all_presence(self.driver, to_select_css, '加载市场分类代码')
            # 全部添加
            self._add_or_delete_all(add_label_css, add_btn_css)
        self.code_loaded = True

    def _choose_code(self, code):
        elem = self.driver.find_element_by_xpath(f"//span[@data-id='{code}']")
        elem.find_element_by_xpath('../i').click()

    def _code_num(self, css):
        n = self.driver.find_element_by_css_selector(css).text
        try:
            return int(n)
        except Exception:
            return 0

    def _delete_selected_code(self):
        css = '.cont-top-right > div:nth-child(3) > div:nth-child(1) > span:nth-child(2) > i:nth-child(1)'
        num = self._code_num(css)
        if num:
            del_label_css = '.cont-top-right > div:nth-child(3) > div:nth-child(1) > label:nth-child(1)'
            del_btn_css = '.cont-top-right > div:nth-child(2) > div:nth-child(1) > button:nth-child(2)'
            self._add_or_delete_all(del_label_css, del_btn_css)

    def _select_all_code(self):
        css = '.cont-top-right > div:nth-child(1) > div:nth-child(1) > span:nth-child(2) > i:nth-child(1)'
        num = self._code_num(css)
        if num:
            add_label_css = '.cont-top-right > div:nth-child(1) > div:nth-child(1) > label:nth-child(1)'
            add_btn_css = '.cont-top-right > div:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
            self._add_or_delete_all(add_label_css, add_btn_css)

    def _reload_all_code(self):
        self._delete_selected_code()
        self._select_all_code()

    def _set_query_codes(self, codes):
        """设置查询代码"""
        self._load_all_code()
        if codes is None:
            self._reload_all_code()
            return
        codes = ensure_list(codes)
        # 将全部股票放入待选区
        self._delete_selected_code()
        # 选择查询代码
        for code in codes:
            self._choose_code(code)
        add_css = '.cont-top-right > div:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
        self.driver.find_element_by_css_selector(add_css).click()

    def _add_or_delete_all(self, label_css, btn_css):
        """添加或删除所选全部元素"""
        # 点击全选元素
        self.driver.find_element_by_css_selector(label_css).click()
        # 点击命令按钮
        self.driver.find_element_by_css_selector(btn_css).click()

    def _before_read(self):
        """等待数据完成加载"""
        css = '.onloading'
        locator = (By.CSS_SELECTOR, css)
        self.wait.until(EC.invisibility_of_element(locator))

    def _loop_by_period(self, level, start, end):
        """分时期段读取数据"""
        loop_str = self.config[level]['date_freq'][0]
        if loop_str is None:
            return self._get_data(None, None)
        ps = self.get_loop_period(level, start, end)
        # 第二个字符指示值的表达格式
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
        res = []
        for s, e in ps:
            t1, t2 = t1_fmt_func(s), t2_fmt_func(e)
            data = self._get_data(t1, t2)
            self.logger.info(f'>  时段 {t1} ~ {t2} 行数 {len(data)}')
            res.extend(data)
        return res

    def _ensure_select_all_fields(self):
        css = '.detail-cont-bottom > div:nth-child(1) > div:nth-child(3) > ul:nth-child(1) li'
        lis = self.driver.find_elements_by_css_selector(css)
        # 只有有待选字段，就执行
        if len(lis):
            self._select_all_fields()

    @property
    def codes(self):
        """当前交易状态中的股票列表"""
        if self._codes is None:
            df = self.get_data('1', codes=None)
            self._codes = df['股票代码'].to_list()
        return self._codes

    def _try_get_data(self, level, start=None, end=None, codes=None):
        start, end = sanitize_dates(start, end)
        if not self.init_driver:
            self._ensure_init()
        self._bt()
        # 务必保持顺序，否则由于屏幕位置滚动导致某些元素不可点击
        self.select_nav(level)  # 1 选择 项目
        self._set_query_codes(codes)  # 2 加载股票代码
        self._ensure_select_all_fields()  # 3 加载字段
        self._log_info('==> ', level, start, end, " <==")
        data = self._loop_by_period(level, start, end)  # 4 设置期间
        columns_mapper = get_field_map('db', level)
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

    def get_data(self, level, start=None, end=None, codes=None):
        """获取项目所有股票期间数据

        Arguments:
            level {str} -- 项目层级

        Keyword Arguments:
            start {str} -- 开始日期 (default: {None})，如为空，使用市场开始日期
            end {str} -- 结束日期 (default: {None})，如为空，使用当前日期
            codes {list like} -- 股票代码 (default: {None})，如为空，使用全部代码


        Usage:
            >>> api = AdvanceSearcher()
            >>> api.get_data('4.1','2018-01-01','2018-08-01')
            >>> api.driver.quit()

        Returns:
            pd.DataFrame -- 如期间没有数据，返回长度为0的空表
        """
        for i in range(1, 6):
            try:
                return self._try_get_data(level, start, end, codes)
            except FutureDate:
                return pd.DataFrame()            
            except Exception as e:
                # 删除请求
                del self.driver.requests
                self.reset()
                self.logger.notice(f"第{i}次尝试\n\n {e!r}")
                time.sleep(random.random())
        raise RetryException(f"尝试失败")
