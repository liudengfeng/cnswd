import json
import random
import re
import time

import pandas as pd
from retry.api import retry_call
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .._seleniumwire import make_headless_browser
from ..setting.config import POLL_FREQUENCY, TIMEOUT
from ..utils.log_utils import make_logger
from .ops import simulated_click

url = 'http://webapi.cninfo.com.cn/#/dataBrowse'
PLATE_TYPE_PAT = re.compile(r'platetype=(.*?)(&|$)')
# PLATE_CODE_PAT = re.compile(r'platecode=(.*?)(&|$)')
INTERVAL = 0.3
PLATE_LEVELS = {
    1: '137004',
    2: '137002',
    3: '137003',
    4: '137006',
    5: '137007',
    6: '137001',
}
PLATE_MAPS = {
    '137001': '市场分类',
    '137002': '证监会行业分类',
    '137003': '国证行业分类',
    '137004': '申万行业分类',
    '137006': '地区分类',
    '137007': '指数分类',
}


def parse_classify_bom(driver,
                       nth,
                       root_css_fmt=".classify-tree > li:nth-child({})"):
    """解析第nth分类方式的基础信息

    Args:
        driver (driver): 无头浏览器
        nth (int): 序号
        root_css_fmt (str, optional): 分类css模板. Defaults to ".classify-tree > li:nth-child({})".

    Returns:
        list: list of dict
    """
    assert nth in range(1, 7)
    root_css = root_css_fmt.format(nth)
    root_tree = driver.find_element_by_css_selector(root_css)
    tree = root_tree.find_elements_by_tag_name("span")
    bom = []
    cate = tree[0].get_attribute('data-name')
    for em in tree[1:]:
        data_id = em.get_attribute('data-id')
        if data_id:
            doc = {
                '分类方式': cate,
                '分类编码': data_id,
                '分类名称': em.get_attribute('data-name'),
            }
            bom.append(doc)
    return sorted(bom, key=lambda x: x['分类编码'])


def parse_classify_tree(driver,
                        nth,
                        root_css_fmt=".classify-tree > li:nth-child({})"):
    """解析第nth分类方式的基础信息

    Args:
        driver (driver): 无头浏览器
        nth (int): 序号
        root_css_fmt (str, optional): 分类css模板. Defaults to ".classify-tree > li:nth-child({})".

    Returns:
        list: list of dict
    """
    assert nth in range(1, 7)
    root_css = root_css_fmt.format(nth)
    root_tree = driver.find_element_by_css_selector(root_css)
    cate = root_tree.text
    tree = root_tree.find_elements_by_tag_name("a")
    res = []
    for em in tree:
        param = em.get_attribute('data-param')
        plate = PLATE_TYPE_PAT.search(param).group(1)
        doc = {
            '分类方式': cate,
            '平台类别': plate,
            '分类编码': em.get_attribute('data-id'),
            '分类名称': em.get_attribute('data-name'),
            'elem': em,
        }
        res.append(doc)
    return res


class ClassifyTree(object):
    """分类树"""
    api_name = '分类树'
    api_e_name = 'dataBrowse'

    def __init__(self):
        self.logger = make_logger(self.api_name)
        self.logger.info("生成无头浏览器......")
        start = time.time()
        self.driver = make_headless_browser()
        self.wait = WebDriverWait(self.driver, TIMEOUT, POLL_FREQUENCY)
        self.driver.get(url)
        # 尝试 driver.refresh()
        btn = self.wait.until(EC.element_to_be_clickable((By.ID, 'btn2')))
        btn.click()
        self.driver.implicitly_wait(INTERVAL)
        self.logger.info(f'加载耗时：{(time.time() - start):>0.4f}秒')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.driver is not None:
            self.driver.quit()

    def yield_classify_tree_bom(self):
        """分类树BOM表"""
        for i in range(1, 7):
            docs = parse_classify_bom(self.driver, i)
            yield docs

    def get_classify_tree(self):
        """获取分类树信息"""
        res = []
        for i in range(1, 7):
            cates = parse_classify_tree(self.driver, i)
            res.extend(cates)
            p = cates[0]['分类方式']
            self.logger.info(f"{p}(共{len(cates)}项)")
        return res

    def get_stock_list(self, elem):
        """分类编码所属股票代码列表

        Args:
            data_id (elem): 分类元素

        Returns:
            list: list of stock code or blank list
        """
        # 模拟点击，发出查询请求
        simulated_click(self, elem)
        request = self.driver.last_request
        # 使用最后请求，等待反馈
        request = self.driver.wait_for_request(request.path, timeout=TIMEOUT)
        response = request.response
        res = []  # 默认为空
        if response.status_code == 200:
            data = json.loads(response.body)
            # 解析结果 402 不合法的参数
            if data['resultcode'] == 200:
                # 只需要股票代码列表
                res = [item['SECCODE'] for item in data['records']]
        del self.driver.requests
        return res

    def yield_classify_tree(self):
        """获取所有分类项下股票代码列表

        Returns:
            list: list of dict
        """
        classify_tree = self.get_classify_tree()
        func = self.get_stock_list
        # 添加分类项下的股票列表
        for doc in classify_tree:
            elem = doc.pop('elem')
            # data_id = doc['分类编码']
            try:
                codes = retry_call(func, [elem],
                                   tries=3,
                                   delay=1,
                                   jitter=(1, 3),
                                   logger=self.logger)
                doc['股票列表'] = codes
                self.logger.info(f"{doc['分类名称']}(股票数量：{len(codes)})")
            except Exception as e:
                doc['异常信息'] = str(e)
                self.logger.exception(f"{doc['分类名称']} \n{e}")
                time.sleep(INTERVAL)
            doc['更新时间'] = pd.Timestamp('now')
            yield doc
            # 避免太频繁请求
            time.sleep(random.uniform(0.30, 0.60))
