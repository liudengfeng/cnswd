import json
import random
import re
import time

from retry.api import retry_call
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

from ..setting.config import TIMEOUT


# region 导航
def sub_menu_list(self, data_pos):
    """层级子菜单"""
    xpath = f'//ul[@data-pos="{data_pos}"]'
    ul = self.driver.find_element_by_xpath(xpath)
    return ul.find_elements_by_tag_name('li')


def parse_level(self, data_pos):
    """递归解析数据项目信息

    Args:
        data_pos (str): 层级编码

    Yields:
        generator: 信息生成器对象
    """
    xpath = f'//li[@data-pos="{data_pos}"]'
    li = self.driver.find_element_by_xpath(xpath)
    class_ = li.get_attribute('class')
    if 'active' not in class_:
        li.click()
    if 'has-child' not in class_:
        elem = li.find_element_by_tag_name('a')
        data_id = elem.get_attribute('data-id')
        name = elem.text
        yield {'层级': data_pos, '数据id': data_id, '名称': name}
    else:
        lis = sub_menu_list(self, data_pos)
        data_pos_list = [li.get_attribute('data-pos') for li in lis]
        for li, data_pos in zip(lis, data_pos_list):
            li.click()
            time.sleep(random.uniform(0.2, 0.5))
            yield from parse_level(self, data_pos)


def get_nav_info(self):
    css = 'div.ul-container:nth-child(2) > ul:nth-child(1)'
    elem = self.driver.find_element_by_css_selector(css)
    lis = elem.find_elements_by_tag_name('li')
    data_pos_list = [li.get_attribute('data-pos') for li in lis]
    return [
        item for data_pos in data_pos_list
        for item in parse_level(self, data_pos)
    ]


def _navigate(self, nums):
    num = ''.join(nums)
    xpath = f'//li[@data-pos="{num}"]'
    li = self.driver.find_element_by_xpath(xpath)
    elem = li.find_element_by_tag_name('a')
    # 将此信息附加到api
    self.api_level = li.get_attribute('data-pos')
    self.current_item = elem.text
    self.current_data_id = elem.get_attribute('data-id')
    elem.click()


def navigate(self, level):
    """逐层导航至菜单

    Args:
        driver (driver)): 浏览器
        level (str): 菜单层级
    """
    nums = list(level)
    for i in range(1, len(nums) + 1):
        _navigate(self, nums[:i])
        time.sleep(random.uniform(0.2, 0.5))


def find_by_id(self, data_id):
    """快速定位分类元素"""
    # 注意:data-id可能重复，并非唯一
    xpath = f"//a[@data-id='{data_id}']"
    # locator = (By.XPATH, xpath)
    # element = self.wait.until(EC.element_to_be_clickable(locator))
    return self.driver.find_element_by_xpath(xpath)


# endregion


# region 模拟操作
def simulated_click(self, em):
    """使用脚本模拟点击"""
    self.driver.execute_script("arguments[0].click();", em)


def input_code(self, code, id_name='input_code'):
    """输入查询代码

    Args:
        driver (driver): 浏览器
        code (str): 完整股票代码
        id_name (str, optional): 元素id. Defaults to 'input_code'.
    """
    elem = self.driver.find_element_by_id(id_name)
    elem.clear()
    elem.send_keys(code)
    css = 'div.searchDataRes:nth-child(2)'
    locator = (By.CSS_SELECTOR, css)
    searched = self.wait.until(EC.element_to_be_clickable(locator))
    result = searched.find_elements_by_tag_name('p')
    if len(result) != 1:
        raise ValueError(f"无效股票代码{code}")
    result[0].click()


def clear_date(self):
    """清除当前光标所在输入框的日期文本"""
    css = '.datepicker-days > table:nth-child(1) > tfoot:nth-child(3) > tr:nth-child(2) > th:nth-child(1)'
    elem = self.driver.find_element_by_css_selector(css)
    simulated_click(self, elem)


def datepicker(self, date_str, css):
    """设定查询期间"""
    elem = self.driver.find_element_by_css_selector(css)
    elem.click()
    clear_date(self)
    elem.send_keys(date_str, Keys.TAB)


def select_year(self, year, id_name='se1'):
    # css = f'#{id_name}_sele'
    # elem = self.driver.find_element_by_css_selector(css)
    # elem.clear()
    # elem.send_keys(year)
    js = "arguments[0].setAttribute('value', arguments[1]);"
    elem = self.driver.find_element_by_id(id_name)
    self.driver.execute_script(js, elem, year)


def select_quarter(self, q, css='.condition2 > select:nth-child(2)'):
    elem = self.driver.find_element_by_css_selector(css)
    t2 = int(q)
    assert t2 in (1, 2, 3, 4), '季度有效值为(1,2,3,4)'
    select = Select(elem)
    select.select_by_index(t2 - 1)


# endregion


# region 解析数据
def find_data_requests(self):
    requests = {}
    api_path = self.get_level_meta_data(self.current_level)['api_path']
    api_key = api_path.replace("http://webapi.cninfo.com.cn", '')
    for r in self.driver.requests:
        if r.method == 'POST' and api_key in r.path:
            requests[r.url] = r
    return requests.values()


def find_request(self, path):
    for r in self.driver.requests:
        if r.method == 'POST' and path in r.url:
            return r


def parse_meta_data(self):
    """解析元数据"""
    meta = {}
    path = f'/api-cloud-platform/apiinfo/info?id={self.current_data_id}'
    request = find_request(self, path)
    response = request.response
    if response.status_code == 200:
        body = request._client.get_response_body(request.id)
        json_data = json.loads(body)
        if json_data['msg'] == 'success':
            data = json_data['data']
            meta['api_level'] = self.api_level
            meta['api_name'] = data['alias']
            meta['apiId'] = data['apiId']
            meta['api_path'] = data['fullUrl']
            meta['inputParameter'] = json.loads(data['inputParameter'])
            meta['field_maps'] = json.loads(data['outputParameter'])
    return meta


def parse_response(self, request):
    """解析指定请求的响应数据

    Returns:
        list: 数据字典列表
    """
    response = request.response
    res = []  # 默认为空
    if response.status_code == 200:
        body = request._client.get_response_body(request.id)
        data = json.loads(body)
        # 解析结果 402 不合法的参数
        if data['resultcode'] == 200:
            # 记录列表
            res = data['records']
    return res


def read_json_data(self):
    """解析查询数据"""
    data = []
    requests = find_data_requests(self)
    for r in requests:
        data.extend(parse_response(self, r))
    # 删除已经读取的请求
    del self.driver.requests
    return data


# endregion


# region 等待
class element_text_change_to(object):
    """期望元素文本改变为指定文本
    """

    def __init__(self, locator, text):
        """
        Arguments:
            locator {定位元组} -- 定位对象  
            text {str} -- 元素文本内容
        """
        self.locator = locator
        self.text = text

    def __call__(self, driver):
        # Finding the referenced element
        element = driver.find_element(*self.locator)
        return self.text == element.text


class element_attribute_change_to(object):
    """期望元素属性更改为指定值
    """

    def __init__(self, locator, name, attribute):
        """
        Arguments:
            locator {定位元组} -- 定位对象  
            name {str} -- 元素属性名称
            attribute {str} -- 属性值
        """
        self.locator = locator
        self.name = name
        self.attribute = attribute

    def __call__(self, driver):
        element = driver.find_element(*self.locator)
        # 当指定名称的属性变更为指定属性时，返回该元素
        if self.attribute == element.get_attribute(self.name):
            return element
        else:
            return False


# endregion
