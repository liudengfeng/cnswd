# coding: utf-8
import pytest
from cnswd.cninfo import FastSearcher, AdvanceSearcher, ThematicStatistics


@pytest.fixture(scope="module")
def fast_api():
    api = FastSearcher()
    api.ensure_init()
    api.nav_tab(api.tab_id)
    return api


@pytest.fixture(scope="module")
def advance_api():
    api = AdvanceSearcher()
    api.ensure_init()
    # 切换到高级搜索
    api.nav_tab(api.tab_id)
    return api


@pytest.fixture(scope="module")
def ts_api():
    api = ThematicStatistics()
    api.ensure_init()
    return api


# @pytest.fixture(scope="session")
# def advance_api():
#     return AdvanceSearcher()
