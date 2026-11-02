from flask import Flask, request, jsonify
import threading
import ssl
import mysql.connector  # Importar mysql.connector para interactuar con MySQL

app = Flask(__name__)

# Configuración de la base de datos MySQL
DB_CONFIG = {
    'user': 'alumno',
    'password': 'alumno',  # Reemplaza con tu contraseña de MySQL
    'host': '127.0.0.1',
    'database': 'EasyCabDB'
}

# Lock para acceso seguro a la base de datos
db_lock = threading.Lock()

def get_db_connection():
    """Obtiene una conexión a la base de datos."""
    return mysql.connector.connect(**DB_CONFIG)

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
    data = request.get_json()
    taxi_id = data.get('taxi_id')
    if not taxi_id:
        return jsonify({'error': 'taxi_id es requerido'}), 400

    try:
        # Verificar si el taxi ya está registrado
        if obtener_taxi_por_id(taxi_id):
            return jsonify({'error': f'Taxi {taxi_id} ya registrado'}), 400

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
    except mysql.connector.Error as err:
        return jsonify({'error': f'Error al registrar taxi: {err}'}), 500
    except Exception as e:
        return jsonify({'error': f'Error inesperado: {e}'}), 500

@app.route('/deregister_taxi/<taxi_id>', methods=['DELETE'])
def deregister_taxi(taxi_id):
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
        except mysql.connector.Error as err:
            return jsonify({'error': f'Error al dar de baja taxi: {err}'}), 500

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
        except mysql.connector.Error as err:
            return jsonify({'error': f'Error al obtener taxis: {err}'}), 500

if __name__ == '__main__':
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('certs/registry_cert.pem', 'certs/registry_key.pem')
    # No es necesario requerir certificados de cliente si no estás utilizando autenticación mutua
    app.run(host='0.0.0.0', port=5003, ssl_context=context)
