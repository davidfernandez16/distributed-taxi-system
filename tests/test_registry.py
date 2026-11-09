import importlib
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


@pytest.fixture
def registry(monkeypatch):
    for name, value in {
        "MYSQL_HOST": "unused", "MYSQL_PORT": "3306",
        "MYSQL_DATABASE": "test", "MYSQL_USER": "test", "MYSQL_PASSWORD": "test",
    }.items():
        monkeypatch.setenv(name, value)
    sys.modules.pop("config", None)
    sys.modules.pop("EC_Registry", None)
    module = importlib.import_module("EC_Registry")
    module.app.config["TESTING"] = True
    return module


def certificate_for(name):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
        .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(minutes=1))
        .sign(key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM).decode("ascii")


def test_register_requires_id(registry):
    response = registry.app.test_client().post("/register_taxi", json={})
    assert response.status_code == 400


def test_register_existing_taxi_is_idempotent(registry, monkeypatch):
    monkeypatch.setattr(registry, "get_verified_taxi_id", lambda: "1")
    monkeypatch.setattr(registry, "obtener_taxi_por_id", lambda taxi_id: {"id": taxi_id})
    response = registry.app.test_client().post("/register_taxi", json={"taxi_id": "1"})
    assert response.status_code == 200


def test_register_new_taxi(registry, monkeypatch):
    monkeypatch.setattr(registry, "get_verified_taxi_id", lambda: "1")
    monkeypatch.setattr(registry, "obtener_taxi_por_id", lambda taxi_id: None)
    cursor = Mock()
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr(registry, "get_db_connection", lambda: connection)
    response = registry.app.test_client().post("/register_taxi", json={"taxi_id": "1"})
    assert response.status_code == 201
    assert cursor.execute.call_args.args[1] == ("1", "BUSY", "RUN", 0, 0)
    connection.commit.assert_called_once()


def test_deregister_unknown_taxi(registry, monkeypatch):
    monkeypatch.setattr(registry, "get_verified_taxi_id", lambda: "1")
    monkeypatch.setattr(registry, "obtener_taxi_por_id", lambda taxi_id: None)
    assert registry.app.test_client().delete("/deregister_taxi/1").status_code == 404


def test_deregister_existing_taxi(registry, monkeypatch):
    monkeypatch.setattr(registry, "get_verified_taxi_id", lambda: "1")
    monkeypatch.setattr(registry, "obtener_taxi_por_id", lambda taxi_id: {"id": taxi_id})
    cursor = Mock()
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr(registry, "get_db_connection", lambda: connection)
    response = registry.app.test_client().delete("/deregister_taxi/1")
    assert response.status_code == 200
    assert cursor.execute.call_args.args[1] == ("1",)
    connection.commit.assert_called_once()


def test_registry_requires_client_certificate(registry, monkeypatch):
    context = Mock()
    monkeypatch.setattr(registry.ssl, "create_default_context", lambda purpose: context)
    assert registry.create_ssl_context() is context
    assert context.verify_mode == registry.ssl.CERT_REQUIRED
    context.load_cert_chain.assert_called_once()
    context.load_verify_locations.assert_called_once()


def test_certificate_identity_is_read_from_tls_environ(registry, monkeypatch):
    with registry.app.test_request_context(
        "/register_taxi", environ_overrides={"SSL_CLIENT_CERT": certificate_for("taxi-1")}
    ):
        assert registry.get_verified_taxi_id() == "1"


def test_matching_certificate_can_register(registry, monkeypatch):
    monkeypatch.setattr(registry, "obtener_taxi_por_id", lambda taxi_id: {"id": taxi_id})
    response = registry.app.test_client().post(
        "/register_taxi", json={"taxi_id": "1"},
        environ_overrides={"SSL_CLIENT_CERT": certificate_for("taxi-1")},
    )
    assert response.status_code == 200


def test_certificate_cannot_register_another_taxi(registry, monkeypatch):
    db_lookup = Mock()
    monkeypatch.setattr(registry, "obtener_taxi_por_id", db_lookup)
    response = registry.app.test_client().post(
        "/register_taxi", json={"taxi_id": "2"},
        environ_overrides={"SSL_CLIENT_CERT": certificate_for("taxi-1")},
    )
    assert response.status_code == 403
    db_lookup.assert_not_called()


def test_certificate_cannot_deregister_another_taxi(registry, monkeypatch):
    db_lookup = Mock()
    monkeypatch.setattr(registry, "obtener_taxi_por_id", db_lookup)
    response = registry.app.test_client().delete(
        "/deregister_taxi/2",
        environ_overrides={"SSL_CLIENT_CERT": certificate_for("taxi-1")},
    )
    assert response.status_code == 403
    db_lookup.assert_not_called()


def test_missing_certificate_is_denied(registry, monkeypatch):
    response = registry.app.test_client().post("/register_taxi", json={"taxi_id": "1"})
    assert response.status_code == 403


def test_register_hides_database_error(registry, monkeypatch):
    monkeypatch.setattr(registry, "get_verified_taxi_id", lambda: "1")
    monkeypatch.setattr(registry, "obtener_taxi_por_id", lambda taxi_id: None)

    def fail_connection():
        raise registry.mysql.connector.Error("internal-database-detail")

    monkeypatch.setattr(registry, "get_db_connection", fail_connection)
    response = registry.app.test_client().post("/register_taxi", json={"taxi_id": "1"})
    assert response.status_code == 500
    assert response.get_json()["error"] == "No se pudo registrar el taxi. Inténtelo más tarde."
    assert "internal-database-detail" not in response.get_data(as_text=True)


def test_deregister_hides_database_error(registry, monkeypatch):
    monkeypatch.setattr(registry, "get_verified_taxi_id", lambda: "1")
    monkeypatch.setattr(registry, "obtener_taxi_por_id", lambda taxi_id: {"id": taxi_id})

    def fail_connection():
        raise registry.mysql.connector.Error("internal-database-detail")

    monkeypatch.setattr(registry, "get_db_connection", fail_connection)
    response = registry.app.test_client().delete("/deregister_taxi/1")
    assert response.status_code == 500
    assert response.get_json()["error"] == "No se pudo dar de baja el taxi. Inténtelo más tarde."
    assert "internal-database-detail" not in response.get_data(as_text=True)


def test_list_hides_database_error(registry, monkeypatch):
    def fail_connection():
        raise registry.mysql.connector.Error("internal-database-detail")

    monkeypatch.setattr(registry, "get_db_connection", fail_connection)
    response = registry.app.test_client().get("/taxis")
    assert response.status_code == 500
    assert response.get_json()["error"] == "No se pudo obtener la lista de taxis. Inténtelo más tarde."
    assert "internal-database-detail" not in response.get_data(as_text=True)
