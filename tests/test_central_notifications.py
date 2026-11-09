"""Prueba la función de consumo sin importar el módulo legado que arranca servidores."""

import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.fixture
def notifications():
    source = Path(__file__).resolve().parents[1] / "EC_Central.py"
    function = next(
        node for node in ast.parse(source.read_text(encoding="utf-8")).body
        if isinstance(node, ast.FunctionDef) and node.name == "recibir_notificaciones_viajes"
    )
    namespace = {
        "broker_ip": "unused", "broker_port": 9092,
        "JsonSerializer": Mock(),
        "validar_token": Mock(),
        "log_auditoria": Mock(),
        "actualizar_estado_cliente": Mock(),
        "enviar_notificacion_cliente": Mock(),
        "mapa_queue": Mock(),
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
    return namespace


def test_invalid_token_cannot_change_trip_state(notifications):
    notifications["validar_token"].return_value = False
    notifications["KafkaConsumer"] = lambda *args, **kwargs: [
        SimpleNamespace(value={
            "taxi_id": "1", "token": "invalid", "customer_id": "1",
            "status": "TRIP_COMPLETED", "destination": [5, 3],
        })
    ]
    notifications["recibir_notificaciones_viajes"]()
    notifications["actualizar_estado_cliente"].assert_not_called()
    notifications["enviar_notificacion_cliente"].assert_not_called()
    notifications["log_auditoria"].assert_called_once()


def test_valid_trip_notification_does_not_expose_token(notifications):
    notifications["validar_token"].return_value = True
    notifications["KafkaConsumer"] = lambda *args, **kwargs: [
        SimpleNamespace(value={
            "taxi_id": "1", "token": "private-token", "customer_id": "1",
            "status": "TRIP_COMPLETED", "destination": [5, 3],
            "mensaje": "Viaje completado",
        })
    ]
    notifications["recibir_notificaciones_viajes"]()
    notifications["validar_token"].assert_called_once_with("1", "private-token")
    sent = notifications["enviar_notificacion_cliente"].call_args.args[0]
    assert sent["status"] == "TRIP_COMPLETED"
    assert sent["destination"] == [5, 3]
    assert "token" not in sent
    notifications["actualizar_estado_cliente"].assert_called_once()


def test_valid_pickup_changes_customer_state(notifications):
    notifications["validar_token"].return_value = True
    notifications["KafkaConsumer"] = lambda *args, **kwargs: [
        SimpleNamespace(value={
            "taxi_id": "1", "token": "private-token", "customer_id": "1",
            "status": "PICKED_UP",
        })
    ]
    notifications["recibir_notificaciones_viajes"]()
    notifications["actualizar_estado_cliente"].assert_called_once_with(
        cliente_id="1", recogido=True,
    )
    notifications["enviar_notificacion_cliente"].assert_not_called()
