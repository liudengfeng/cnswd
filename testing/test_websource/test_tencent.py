from cnswd.websource.tencent import get_recent_trading_stocks


def test_get_recent_trading_stocks():
    codes = get_recent_trading_stocks()
    assert isinstance(codes, list)
    assert len(codes) >= 3000
