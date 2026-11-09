from flask import Flask, request, jsonify
import threading
import ssl
import mysql.connector  # Importar mysql.connector para interactuar con MySQL
from config import DB_CONFIG
import os
import re
import logging
from cryptography import x509
from cryptography.x509.oid import NameOID

app = Flask(__name__)
logger = logging.getLogger(__name__)
REGISTRY_CERT_FILE = os.environ.get(
    "REGISTRY_CERT_FILE",
    "/run/certs/registry.crt",
)
REGISTRY_KEY_FILE = os.environ.get(
    "REGISTRY_KEY_FILE",
    "/run/certs/registry.key",
)
CA_CERT_FILE = os.environ.get(
    "CA_CERT_FILE",
    "/run/certs/ca.crt",
)


# Lock para acceso seguro a la base de datos
db_lock = threading.Lock()

def get_db_connection():
    """Obtiene una conexión a la base de datos."""
    return mysql.connector.connect(**DB_CONFIG)

def get_verified_taxi_id():
    """Obtiene el ID del certificado de cliente verificado por el servidor TLS."""
    pem = request.environ.get("SSL_CLIENT_CERT")
    if not pem:
        return None
    try:
        certificate = x509.load_pem_x509_certificate(pem.encode("ascii"))
        names = certificate.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    except (ValueError, UnicodeEncodeError):
        return None
    if len(names) != 1:
        return None
    match = re.fullmatch(r"taxi-(\d+)", names[0].value)
    return match.group(1) if match else None

def obtener_taxi_por_id(taxi_id):
    with db_lock:
        cnx = get_db_connection()
        cursor = cnx.cursor(dictionary=True)
        query = "SELECT * FROM Taxis WHERE id = %s"
        cursor.execute(query, (taxi_id,))
        taxi = cursor.fetchone()
        cursor.close()
        cnx.close()
        return taxi

@app.route('/register_taxi', methods=['POST'])
def register_taxi():
    data = request.get_json(silent=True)
    taxi_id = data.get('taxi_id') if isinstance(data, dict) else None
    if not taxi_id:
        return jsonify({'error': 'taxi_id es requerido'}), 400
    if get_verified_taxi_id() != str(taxi_id):
        return jsonify({'error': 'Certificado no autorizado para este taxi'}), 403

    try:
        # Verificar si el taxi ya está registrado
        if obtener_taxi_por_id(taxi_id):
            return jsonify({
                "message": f"Taxi {taxi_id} ya estaba registrado"
        }), 200

        with db_lock:
            cnx = get_db_connection()
            cursor = cnx.cursor()
            query = """
            INSERT INTO Taxis (id, availability, movement, localizacion_x, localizacion_y)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(query, (taxi_id, 'BUSY', 'RUN', 0, 0))
            cnx.commit()
            cursor.close()
            cnx.close()
        return jsonify({'message': f'Taxi {taxi_id} registrado exitosamente'}), 201
    except mysql.connector.Error:
        logger.exception("Error de base de datos al registrar un taxi")
        return jsonify({'error': 'No se pudo registrar el taxi. Inténtelo más tarde.'}), 500
    except Exception:
        logger.exception("Error inesperado al registrar un taxi")
        return jsonify({'error': 'No se pudo registrar el taxi. Inténtelo más tarde.'}), 500

@app.route('/deregister_taxi/<taxi_id>', methods=['DELETE'])
def deregister_taxi(taxi_id):
    if get_verified_taxi_id() != taxi_id:
        return jsonify({'error': 'Certificado no autorizado para este taxi'}), 403
    if not obtener_taxi_por_id(taxi_id):
        return jsonify({'error': f'Taxi {taxi_id} no encontrado'}), 404

    with db_lock:
        try:
            cnx = get_db_connection()
            cursor = cnx.cursor()
            query = "DELETE FROM Taxis WHERE id = %s"
            cursor.execute(query, (taxi_id,))
            cnx.commit()
            cursor.close()
            cnx.close()
            return jsonify({'message': f'Taxi {taxi_id} dado de baja exitosamente'}), 200
        except mysql.connector.Error:
            logger.exception("Error de base de datos al dar de baja un taxi")
            return jsonify({'error': 'No se pudo dar de baja el taxi. Inténtelo más tarde.'}), 500
        except Exception:
            logger.exception("Error inesperado al dar de baja un taxi")
            return jsonify({'error': 'No se pudo dar de baja el taxi. Inténtelo más tarde.'}), 500

@app.route('/taxis', methods=['GET'])
def get_taxis():
    with db_lock:
        try:
            cnx = get_db_connection()
            cursor = cnx.cursor(dictionary=True)
            cursor.execute("SELECT * FROM Taxis")
            taxis = cursor.fetchall()
            cursor.close()
            cnx.close()
            return jsonify(taxis), 200
        except mysql.connector.Error:
            logger.exception("Error de base de datos al obtener los taxis")
            return jsonify({'error': 'No se pudo obtener la lista de taxis. Inténtelo más tarde.'}), 500
        except Exception:
            logger.exception("Error inesperado al obtener los taxis")
            return jsonify({'error': 'No se pudo obtener la lista de taxis. Inténtelo más tarde.'}), 500

def create_ssl_context():
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(
        certfile=REGISTRY_CERT_FILE,
        keyfile=REGISTRY_KEY_FILE,
    )
    context.load_verify_locations(cafile=CA_CERT_FILE)
    context.verify_mode = ssl.CERT_REQUIRED
    return context

if __name__ == "__main__":
    ssl_context = create_ssl_context()
    app.run(host="0.0.0.0", port=5003, ssl_context=ssl_context)
