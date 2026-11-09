import os


def required_env(name):
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(
            f"La variable de entorno {name} es obligatoria."
        )

    return value


def required_port(name):
    value = required_env(name)

    try:
        return int(value)
    except ValueError:
        raise RuntimeError(
            f"La variable de entorno {name} debe ser un número entero."
        ) from None


DB_CONFIG = {
    "host": required_env("MYSQL_HOST"),
    "port": required_port("MYSQL_PORT"),
    "database": required_env("MYSQL_DATABASE"),
    "user": required_env("MYSQL_USER"),
    "password": required_env("MYSQL_PASSWORD"),
}

CTC_URL = os.environ.get(
    "CTC_URL",
    "http://ctc:5001/traffic_status",
)