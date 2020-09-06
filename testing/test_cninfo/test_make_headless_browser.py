from cnswd._seleniumwire import make_headless_browser
import pytest


@pytest.mark.parametrize("url,expected", [
    ("https://www.sohu.com/", "搜狐"),
    ("https://www.sina.com.cn/", "新浪首页"),
    ("http://webapi.cninfo.com.cn/#/dataBrowse", "深证信数据服务平台"),
])
def test_headless_browser(url, expected):
    with make_headless_browser() as driver:
        driver.get(url)
        assert driver.title == expected
