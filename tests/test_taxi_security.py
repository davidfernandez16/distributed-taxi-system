import importlib
from unittest.mock import Mock


def test_taxi_loads_exact_binary_key(monkeypatch, tmp_path):
    taxi = importlib.import_module("EC_DE")
    key = b"\x20" + bytes(range(1, 31)) + b"\x0a"
    key_file = tmp_path / "message.key"
    key_file.write_bytes(key)
    monkeypatch.setattr(taxi, "TAXI_MESSAGE_KEY_FILE", str(key_file))
    taxi.cargar_clave_taxi()
    assert taxi.TAXI_KEY == key


def test_taxi_register_uses_mutual_tls(monkeypatch):
    taxi = importlib.import_module("EC_DE")
    monkeypatch.setattr(taxi, "taxi_id", "1")
    monkeypatch.setattr(taxi, "registry_ip", "registry")
    monkeypatch.setattr(taxi, "registry_port", 5003)
    monkeypatch.setattr(taxi, "registered", False)
    response = Mock(status_code=200)
    post = Mock(return_value=response)
    monkeypatch.setattr(taxi.requests, "post", post)
    taxi.registrar_taxi()
    assert taxi.registered
    assert post.call_args.kwargs["cert"] == (taxi.TAXI_CERT_FILE, taxi.TAXI_KEY_FILE)
    assert post.call_args.kwargs["verify"] == taxi.CA_CERT_FILE
    assert post.call_args.kwargs["timeout"] == taxi.REQUEST_TIMEOUT


def test_taxi_deregister_uses_mutual_tls(monkeypatch):
    taxi = importlib.import_module("EC_DE")
    monkeypatch.setattr(taxi, "taxi_id", "1")
    monkeypatch.setattr(taxi, "registry_ip", "registry")
    monkeypatch.setattr(taxi, "registry_port", 5003)
    response = Mock(status_code=200)
    delete = Mock(return_value=response)
    monkeypatch.setattr(taxi.requests, "delete", delete)
    taxi.dar_de_baja_taxi()
    assert delete.call_args.kwargs["cert"] == (taxi.TAXI_CERT_FILE, taxi.TAXI_KEY_FILE)
    assert delete.call_args.kwargs["verify"] == taxi.CA_CERT_FILE
    assert delete.call_args.kwargs["timeout"] == taxi.REQUEST_TIMEOUT
