"""Pruebas opcionales contra Compose: EASYCAB_INTEGRATION=1 pytest -q."""

import os

import pytest
import requests


pytestmark = pytest.mark.skipif(
    os.environ.get("EASYCAB_INTEGRATION") != "1",
    reason="Requiere los servicios Docker activos y EASYCAB_INTEGRATION=1",
)


def test_central_exposes_map():
    response = requests.get(
        os.environ.get("CENTRAL_URL", "http://central:5002") + "/mapa",
        timeout=10,
    )
    response.raise_for_status()
    assert {"clientes", "locations", "taxis"} <= response.json().keys()


def test_registry_accepts_provisioned_taxi_certificate():
    response = requests.get(
        os.environ.get("REGISTRY_URL", "https://registry:5003") + "/taxis",
        cert=(
            os.environ.get("TAXI_CERT_FILE", "/secrets/taxi-1/taxi-1.crt"),
            os.environ.get("TAXI_KEY_FILE", "/secrets/taxi-1/taxi-1.key"),
        ),
        verify=os.environ.get("CA_CERT_FILE", "/secrets/ca/ca.crt"),
        timeout=10,
    )
    response.raise_for_status()
    assert isinstance(response.json(), list)
