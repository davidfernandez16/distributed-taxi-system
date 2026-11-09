import importlib
import sys
from unittest.mock import Mock

import pytest
import requests


@pytest.fixture
def ctc(monkeypatch):
    monkeypatch.setenv("OPENWEATHER_API_KEY", "test-key")
    sys.modules.pop("EC_CTC", None)
    module = importlib.import_module("EC_CTC")
    module.app.config["TESTING"] = True
    monkeypatch.setattr(module, "ciudad_actual", "Alicante")
    monkeypatch.setattr(module, "umbral_temperatura", 0)
    return module


@pytest.mark.parametrize("temperature,expected", [(21.5, "OK"), (-2.0, "KO"), (0, "OK")])
def test_traffic_status(ctc, monkeypatch, temperature, expected):
    weather = Mock(status_code=200)
    weather.json.return_value = {"main": {"temp": temperature}}
    get = Mock(return_value=weather)
    monkeypatch.setattr(ctc.requests, "get", get)
    response = ctc.app.test_client().get("/traffic_status")
    assert response.status_code == 200
    assert response.json["status"] == expected
    assert response.json["temp"] == temperature
    get.assert_called_once_with(
        ctc.WEATHER_API_URL,
        params={"q": "Alicante", "appid": "test-key", "units": "metric"},
        timeout=ctc.REQUEST_TIMEOUT,
    )


def test_unknown_city(ctc, monkeypatch):
    monkeypatch.setattr(ctc.requests, "get", lambda *a, **kw: Mock(status_code=404))
    response = ctc.app.test_client().get("/traffic_status")
    assert response.status_code == 404
    assert response.json["error"] == "Ciudad no encontrada"


@pytest.mark.parametrize("failure", [requests.Timeout("private detail"), ValueError("private detail")])
def test_weather_failures_do_not_expose_details(ctc, monkeypatch, failure):
    def fail(*args, **kwargs):
        raise failure
    monkeypatch.setattr(ctc.requests, "get", fail)
    response = ctc.app.test_client().get("/traffic_status")
    assert response.status_code == 502
    assert "private detail" not in response.get_data(as_text=True)


def test_missing_weather_key_fails_at_startup(monkeypatch):
    monkeypatch.delenv("OPENWEATHER_API_KEY", raising=False)
    sys.modules.pop("EC_CTC", None)
    with pytest.raises(RuntimeError, match="OPENWEATHER_API_KEY"):
        importlib.import_module("EC_CTC")
