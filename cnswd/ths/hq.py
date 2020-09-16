"""
股票分类信息

+ 同花顺概念
+ 同花顺行业
+ 地域分类
+ 证监会行业分类
"""
import pandas as pd
from dataclasses import dataclass
from selenium.common.exceptions import NoSuchElementException
from .base import THS

base_url = 'http://q.10jqka.com.cn/'
# 同花顺概念、同花顺行业、地域、证监会行业分类
titles = ['gn', 'thshy', 'dy', 'zjhhy']


@dataclass
class GNINFO:
    url: str
    日期: pd.Timestamp
    编码: int
    名称: str
    驱动事件: str
    概念定义: str
    股票列表: list


@dataclass
class INFO:
    url: str
    编码: int
    名称: str
    股票列表: list


class THSHQ(THS):
    """同花顺行情基础类"""
    path = '**'
    url = f"{base_url}{path}/"
    api_name = ''
    # 定义读取表属性
    table_attrs = {'class': 'm-table m-pager-table'}

    def _load_page(self, url=None):
        """加载网页"""
        current_url = self.url if url is None else url
        self.logger.info(f"加载：{current_url}")
        self.driver.get(current_url)
        request = self._wait_path_loading(current_url)
        if request.response.status_code != 200:
            raise ValueError(request.response.reason)

    def _parse_stock_code_list_in_page(self):
        """解析当页所含股票代码列表"""
        tr_css = '.m-table tbody tr'
        trs = self.driver.find_elements_by_css_selector(tr_css)
        codes = []
        for tr in trs:
            # 部分列表为空
            try:
                code = tr.find_element_by_css_selector(':nth-child(2)').text
                codes.append(code)
            except NoSuchElementException:
                pass
        del self.driver.requests
        return codes

    def _parse_head_in_page(self):
        raise NotImplementedError('必须在子类中完成')

    def get_categories(self):
        """获取分类汇总列表"""
        self._load_page()
        head = []
        page_num = self.get_page_num()
        first = self._parse_head_in_page()
        if page_num == 1:
            head = first
        else:
            head.extend(first)
            for p in range(2, page_num+1):
                self._change_page_to(p)
                path = f'page/{p}'
                self._wait_path_loading(path)
                head.extend(self._parse_head_in_page())
        return head

    def _update_detail(self, info):
        """更新单个项目信息"""
        url = info.url
        self._load_page(url)

        # 股票概念需要提取概念定义信息
        info = self._add_update(info)

        page_num = self.get_page_num()
        codes = []
        first_codes = self._parse_stock_code_list_in_page()
        if page_num == 1:
            codes = first_codes
            page_info = f"1/{page_num}"
            self.logger.info(
                f"{self.api_name} {info.名称} {page_info:>8} {len(codes):>5} 行")
        else:
            codes.extend(first_codes)
            for p in range(2, page_num+1):
                self._change_page_to(p)
                path = f'page/{p}'
                self._wait_path_loading(path)
                page_codes = self._parse_stock_code_list_in_page()
                codes.extend(page_codes)
                page_info = f"{p}/{page_num}"
                self.logger.info(
                    f"{self.api_name} {info.名称} {page_info:>8} {len(codes):>5} 行")
        info.股票列表 = codes
        del self.driver.requests
        return info

    def _add_update(self, info):
        return info

    def get_stock_code_list_of_category(self, info):
        """项目股票列表信息"""
        return self._update_detail(info)


class THSGN(THSHQ):
    """同花顺概念"""
    path = 'gn'
    url = f"{base_url}{path}/"
    api_name = "同花顺概念"

    def _parse_head_in_page(self):
        """解析概念汇总信息"""
        tr_css = '.m-table tbody tr'
        trs = self.driver.find_elements_by_css_selector(tr_css)
        dates = [tr.find_element_by_css_selector(
            ':first-child').text for tr in trs]
        events = [tr.find_element_by_css_selector(
            ':nth-child(3)').text for tr in trs]
        dates = pd.to_datetime(dates)
        urls = self._parse_href_in_table()
        res = []
        for date, event, key in zip(dates, events, urls):
            res.append(
                GNINFO(
                    url=urls[key],
                    日期=date,
                    名称=key,
                    编码=urls[key].split('/')[-2],
                    驱动事件=event,
                    概念定义='',
                    股票列表=[],
                )
            )
        del self.driver.requests
        return res

    def _add_update(self, info):
        definition_css = '.board-txt > p:nth-child(2)'
        info.概念定义 = self.driver.find_element_by_css_selector(
            definition_css).text
        return info


class THSDY(THSHQ):
    """同花顺股票所属地域"""
    path = 'dy'
    url = f"{base_url}{path}/"
    api_name = "股票所属地域"

    def _parse_head_in_page(self):
        """解析地域汇总信息"""
        urls = self._parse_href_in_table()
        res = []
        for name, url in urls.items():
            res.append(
                INFO(
                    url=url,
                    名称=name,
                    编码=url.split('/')[-2],
                    股票列表=[],
                )
            )
        del self.driver.requests
        return res


class THSHY(THSHQ):
    """同花顺行业"""
    path = 'thshy'
    url = f"{base_url}{path}/"
    api_name = "同花顺行业分类"

    def _parse_head_in_page(self):
        """解析同花顺行业分类"""
        urls = self._parse_href_in_table()
        res = []
        for name, url in urls.items():
            res.append(
                INFO(
                    url=url,
                    名称=name,
                    编码=url.split('/')[-2],
                    股票列表=[],
                )
            )
        del self.driver.requests
        return res


class ZJHHY(THSHQ):
    """证监会行业"""
    path = 'zjhhy'
    url = f"{base_url}{path}/"
    api_name = "证监会行业分类"

    def _parse_head_in_page(self):
        """解析证监会行业分类"""
        urls = self._parse_href_in_table()
        res = []
        for name, url in urls.items():
            res.append(
                INFO(
                    url=url,
                    名称=name,
                    编码=url.split('/')[-2],
                    股票列表=[],
                )
            )
        del self.driver.requests
        return res
