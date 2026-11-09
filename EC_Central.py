import socket
import requests
import time
import threading
from flask_socketio import SocketIO
import queue
import sys
from kafka import KafkaProducer, KafkaConsumer, JsonSerializer
import json
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import os
import mysql.connector  # Importar mysql.connector para interactuar con MySQL
from flask import Flask, jsonify
import threading
from flask_cors import CORS
import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)  # Cambia a CRITICAL para ocultar todos los mensajes
import uuid
from crypto_utils import encrypt_message, decrypt_message
import datetime
from config import DB_CONFIG, CTC_URL



ultimo_estado_trafico = None
# Estructura global para tokens (token -> taxi_id) o (taxi_id -> token), según la necesidad.
# Aquí usamos taxi_id -> token para fácil consulta.
tokens_activos = {}
CLAVES_TAXIS = {}
TAXI_KEYS_DIR = os.environ.get(
    "TAXI_KEYS_DIR",
    "/run/taxi-keys",
)

def log_auditoria(ip, action, description):
    # ip: dirección IP de la fuente del evento
    # action: acción realizada (p.ej. "Autenticación exitosa", "Tráfico KO", etc.)
    # description: parámetros o detalles del evento
    
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Registramos en un archivo de texto
    with open("auditoria.log", "a") as f:
        f.write(f"{now}|{ip}|{action}|{description}\n")


def emitir_actualizacion_mapa():
    taxis = cargar_taxis_db()
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    cursor.execute("SELECT * FROM Clientes")
    clientes = cursor.fetchall()
    cursor.execute("SELECT * FROM Locations")
    locations = cursor.fetchall()
    cursor.close()
    cnx.close()

    data = {"taxis": taxis, "clientes": clientes, "locations": locations}
    socketio.emit('update_map', data)

def cargar_claves_taxis():
    # Supongamos que los taxis tienen IDs del 1 al 10, o se pueden cargar dinámicamente.
    # Para simplicidad, cargaremos las claves a demanda.
    pass

def get_key_for_taxi(taxi_id):
    # Si la clave no está en el diccionario, la cargamos
    key_file = os.path.join(TAXI_KEYS_DIR, f"taxi-{taxi_id}.key")
    if not os.path.exists(key_file):
        print(f"Error: No se encuentra la clave para el taxi {taxi_id}")
        return None
    with open(key_file, "rb") as f:
        key = f.read()
    if len(key) not in (16,24,32):
        print(f"Error: La clave del taxi {taxi_id} no es válida.")
        return None
    return key


def generar_token():
    return str(uuid.uuid4())

def asignar_token_a_taxi(taxi_id):
    token = generar_token()
    tokens_activos[taxi_id] = token
    return token

def validar_token(taxi_id, token):
    return tokens_activos.get(taxi_id) == token

def invalidar_token_taxi(taxi_id):
    if taxi_id in tokens_activos:
        del tokens_activos[taxi_id]




producer = None  # Productor de Kafka para enviar respuestas
mapa_queue = queue.Queue()


def get_db_connection():
    """Obtiene una conexión a la base de datos."""
    return mysql.connector.connect(**DB_CONFIG)

def verificar_taxi_registrado(taxi_id):
    taxi = obtener_taxi_por_id(taxi_id)
    return taxi is not None

def verificar_trafico():
    global ultimo_estado_trafico
    while True:
        try:
            respuesta = requests.get(CTC_URL, timeout=10)
            datos = respuesta.json()
            estado_actual = datos["status"]

            if estado_actual != ultimo_estado_trafico:
                # Solo actuar e imprimir si el estado ha cambiado
                if estado_actual == "KO":
                    print(f"[ALERTA] Tráfico no viable: {datos['message']}. Temperatura: {datos['temp']}°C")
                    suspender_operaciones()
                    # No hay IP específica, podemos poner "Sistema" o una IP genérica (0.0.0.0)
                    log_auditoria("0.0.0.0", "Cambio de situación de tráfico", "Tráfico no viable - KO")

                else:
                    print(f"[INFO] Tráfico viable: {datos['message']}. Temperatura: {datos['temp']}°C")
                    reanudar_operaciones()
                    log_auditoria("0.0.0.0", "Cambio de situación de tráfico", "Tráfico viable - OK")


                ultimo_estado_trafico = estado_actual
            # Si el estado no ha cambiado, no imprimir nada ni reanudar/suspender otra vez

        except Exception as e:
            print(f"[ERROR] No se pudo conectar con EC_CTC: {e}")
        time.sleep(10)  # Revisar cada 10 segundos

def reanudar_operaciones():
    print("Reanudando operaciones. Cambiando estado de taxis a 'AVAILABLE'.")
    taxis = cargar_taxis_db()
    for taxi in taxis:
        if taxi["availability"] == "BUSY" and taxi["localizacion_x"] == 0 and taxi["localizacion_y"] == 0:
            actualizar_estado_y_posicion_taxi(taxi["id"], availability="AVAILABLE")

def suspender_operaciones():
    print("Suspendiendo operaciones y enviando taxis a la base.")
    taxis = cargar_taxis_db()
    for taxi in taxis:
        taxi_id = taxi["id"]
        enviar_orden_taxi_regresar_base(taxi_id)
        actualizar_estado_y_posicion_taxi(taxi_id, availability="BUSY", movement="RUN", localizacion=(0, 0))
        # Una vez que el taxi retorne a la base y se desconecte, invalidamos el token:
        invalidar_token_taxi(taxi_id)

def enviar_orden_taxi_regresar_base(taxi_id):
    orden = {
        "taxi_id": taxi_id,
        "mensaje": "VOLVER_BASE",
        "base": [0, 0],
        # Incluir token actual para que taxi sepa a quién se dirige (opcional)
        # No es necesario si la orden va unidireccional. Pero normalmente 
        # la central no necesita token para enviar orden. El token es para verificar 
        # al taxi, no al revés.
    }
    # El envío sigue igual
    try:
        producer.send('taxi_requests', value=orden)
        producer.flush()
        print(f"Orden enviada al taxi {taxi_id} para regresar a la base.")
    except Exception as e:
        print(f"Error al enviar orden al taxi {taxi_id}: {e}")


# Funciones para clientes
def obtener_cliente_por_id(cliente_id):
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    query = "SELECT * FROM Clientes WHERE id = %s"
    cursor.execute(query, (cliente_id,))
    cliente = cursor.fetchone()
    cursor.close()
    cnx.close()
    return cliente


app = Flask(__name__)
CORS(app)

@app.route('/taxis', methods=['GET'])
def obtener_taxis():
    """Devuelve el estado de todos los taxis."""
    try:
        taxis = cargar_taxis_db()
        return jsonify(taxis), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/clientes', methods=['GET'])
def obtener_clientes():
    """Devuelve el estado de todos los clientes."""
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor(dictionary=True)
        cursor.execute("SELECT * FROM Clientes")
        clientes = cursor.fetchall()
        cursor.close()
        cnx.close()
        return jsonify(clientes), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/mapa', methods=['GET'])
def obtener_mapa():
    """Devuelve el estado actual del mapa."""
    try:
        taxis = cargar_taxis_db()
        cnx = get_db_connection()
        cursor = cnx.cursor(dictionary=True)
        cursor.execute("SELECT * FROM Clientes")
        clientes = cursor.fetchall()
        cursor.execute("SELECT * FROM Locations")
        locations = cursor.fetchall()
        cursor.close()
        cnx.close()

        return jsonify({"taxis": taxis, "clientes": clientes, "locations": locations}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def actualizar_estado_cliente(cliente_id, recogido, destination=None, localizacion=None):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    updates = []
    params = []
    updates.append("recogido = %s")
    params.append(recogido)
    if destination is not None:
        updates.append("destination_x = %s, destination_y = %s")
        params.extend(destination)
    if localizacion is not None:
        updates.append("localizacion_x = %s, localizacion_y = %s")
        params.extend(localizacion)
    params.append(cliente_id)
    update_query = "UPDATE Clientes SET " + ", ".join(updates) + " WHERE id = %s"
    cursor.execute(update_query, params)
    cnx.commit()
    cursor.close()
    cnx.close()

# Funciones para ubicaciones
def obtener_coordenadas_por_id(destino_id):
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    query = "SELECT * FROM Locations WHERE id = %s"
    cursor.execute(query, (destino_id,))
    location = cursor.fetchone()
    cursor.close()
    cnx.close()
    if location:
        return [location['pos_x'], location['pos_y']]
    else:
        return None

# Configuración del productor de Kafka
def setup_kafka(broker_ip, broker_port):
    global producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=f'{broker_ip}:{broker_port}',
            value_serializer=JsonSerializer(),
            retries=5,
            acks='all'
        )
        print("[INFO] Kafka Producer configurado correctamente.")
    except Exception as e:
        print(f"[ERROR] No se pudo inicializar Kafka Producer: {e}")
        sys.exit(1)

# Funciones para taxis
def cargar_taxis_db():
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    query = "SELECT * FROM Taxis"
    cursor.execute(query)
    taxis = cursor.fetchall()
    cursor.close()
    cnx.close()
    return taxis

def obtener_taxi_por_id(taxi_id):
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    query = "SELECT * FROM Taxis WHERE id = %s"
    cursor.execute(query, (taxi_id,))
    taxi = cursor.fetchone()
    cursor.close()
    cnx.close()
    return taxi

def agregar_o_actualizar_taxi(taxi_id, availability="AVAILABLE", movement="RUN", localizacion=(0, 0)):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    taxi = obtener_taxi_por_id(taxi_id)
    if taxi:
        query = """
        UPDATE Taxis SET availability = %s, movement = %s, localizacion_x = %s, localizacion_y = %s WHERE id = %s
        """
        cursor.execute(query, (availability, movement, localizacion[0], localizacion[1], taxi_id))
    else:
        query = """
        INSERT INTO Taxis (id, availability, movement, localizacion_x, localizacion_y) VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (taxi_id, availability, movement, localizacion[0], localizacion[1]))
    cnx.commit()
    cursor.close()
    cnx.close()

def obtener_taxi_disponible():
    cnx = get_db_connection()
    cursor = cnx.cursor(dictionary=True)
    query = "SELECT * FROM Taxis WHERE availability = 'AVAILABLE' LIMIT 1"
    cursor.execute(query)
    taxi = cursor.fetchone()
    cursor.close()
    cnx.close()
    return taxi

def actualizar_estado_y_posicion_taxi(taxi_id, availability=None, movement=None, localizacion=None):
    cnx = get_db_connection()
    cursor = cnx.cursor()
    updates = []
    params = []
    if availability:
        updates.append("availability = %s")
        params.append(availability)
    if movement:
        updates.append("movement = %s")
        params.append(movement)
    if localizacion:
        updates.append("localizacion_x = %s, localizacion_y = %s")
        params.extend(localizacion)
    params.append(taxi_id)
    update_query = "UPDATE Taxis SET " + ", ".join(updates) + " WHERE id = %s"
    cursor.execute(update_query, params)
    cnx.commit()
    cursor.close()
    cnx.close()
    emitir_actualizacion_mapa()

# Función para mostrar el mapa gráfico
def mostrar_mapa_grafico():
    plt.ion()  # Enable interactive mode
    fig, ax = plt.subplots(figsize=(10, 10))  # Adjusted size for better visualization
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 20)
    ax.invert_yaxis()

    def actualizar_mapa():
        ax.clear()
        ax.set_xlim(0, 20)
        ax.set_ylim(0, 20)
        ax.invert_yaxis()

        # Draw the grid cells
        for x in range(20):
            for y in range(20):
                ax.add_patch(patches.Rectangle((x, y), 1, 1, fill=True, facecolor='white', edgecolor='black'))

        # Display locations
        cnx = get_db_connection()
        cursor = cnx.cursor(dictionary=True)
        cursor.execute("SELECT * FROM Locations")
        locations = cursor.fetchall()
        cursor.close()
        cnx.close()
        for location in locations:
            x = location['pos_x'] % 20
            y = location['pos_y'] % 20
            ax.add_patch(patches.Rectangle((x, y), 1, 1, fill=True, facecolor='blue', edgecolor='black'))
            ax.text(x + 0.5, y + 0.5, location['id'], ha='center', va='center', fontsize=12, color='white', weight='bold')

        # Display clients
        cnx = get_db_connection()
        cursor = cnx.cursor(dictionary=True)
        cursor.execute("SELECT * FROM Clientes")
        clientes = cursor.fetchall()
        cursor.close()
        cnx.close()
        for cliente in clientes:
            if not cliente['recogido']:
                x = cliente['localizacion_x'] % 20
                y = cliente['localizacion_y'] % 20
                ax.add_patch(patches.Rectangle((x, y), 1, 1, fill=True, facecolor='yellow', edgecolor='black'))
                ax.text(x + 0.5, y + 0.5, str(cliente['id']), ha='center', va='center', fontsize=12, color='black', weight='bold')

        # Display taxis
        taxis = cargar_taxis_db()
        for taxi in taxis:
            x = taxi['localizacion_x'] % 20
            y = taxi['localizacion_y'] % 20
            color = 'green' if taxi['availability'] == "AVAILABLE" else 'red'
            ax.add_patch(patches.Rectangle((x, y), 1, 1, fill=True, facecolor=color, edgecolor='black'))
            ax.text(x + 0.5, y + 0.5, str(taxi['id']), ha='center', va='center', fontsize=12, color='white', weight='bold')

        plt.draw()

    # Update loop based on queue
    while True:
        if not mapa_queue.empty():
            mapa_queue.get()
            actualizar_mapa()
        plt.pause(0.1)


def handle_taxi_auth(conn, addr):
    try:
        message = conn.recv(1024).decode().strip()
        if not message:
            print(f"Conexión cerrada por el taxi desde {addr} o mensaje vacío.")
            return

        print(f"Mensaje recibido para autenticación: '{message}'")

        try:
            taxi_data = json.loads(message)
            taxi_id = taxi_data["taxi_id"]

            # Verificar si el taxi está registrado
            if not verificar_taxi_registrado(taxi_id):
                response = "KO#Taxi no registrado en el sistema"
                conn.send(response.encode())
                # En el except o en caso de KO:
                taxi_ip = addr[0]
                log_auditoria(taxi_ip, "Error de autenticación", f"Fallo al autenticar taxi {taxi_id}")

                return

            position = taxi_data["position"]
            movement = taxi_data["movement"]

            agregar_o_actualizar_taxi(taxi_id, availability="AVAILABLE", movement=movement, localizacion=position)
            
            # Generar token y enviarlo al taxi
            token = asignar_token_a_taxi(taxi_id)
            response = f"OK#Taxi autenticado y listo#Token:{token}"
            conn.send(response.encode())
            # Después de conn.send(response.encode()) en caso de éxito:
            # Obtener la IP del taxi desde addr (addr[0] es la IP)
            taxi_ip = addr[0]
            log_auditoria(taxi_ip, "Autenticación taxi", f"Taxi {taxi_id} autenticado con token")

            mapa_queue.put("update")

        except json.JSONDecodeError as e:
            print(f"Error al procesar el mensaje del taxi desde {addr}: {e}")
            conn.send("KO#Error en autenticación".encode())
        except Exception as e:
            print(f"Error desconocido al procesar el mensaje del taxi desde {addr}: {e}")
            conn.send("KO#Error en autenticación".encode())

    except Exception as e:
        print(f"Error en la conexión con el taxi {addr}: {e}")
    finally:
        conn.close()


def start_taxi_auth_server(central_ip, central_port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((central_ip, central_port))
    server.listen(5)
    print(f"Servidor de autenticación de taxis iniciado en {central_ip}:{central_port}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_taxi_auth, args=(conn, addr))
        thread.start()

def manejar_solicitud_taxi(solicitud_encriptada):
    
    # Cargamos las claves de todos los taxis registrados en BD
    taxis = cargar_taxis_db()
    for t in taxis:
        tid = str(t['id'])
        key = get_key_for_taxi(tid)
        if key is None:
            continue
        try:
            decrypted = decrypt_message(solicitud_encriptada, key)
            solicitud = json.loads(decrypted.decode('utf-8'))
            # Si llegamos aquí, descifró con éxito y tenemos taxi_id
            taxi_id = str(solicitud.get("taxi_id"))
            token = solicitud.get("token")

            if not token or not validar_token(taxi_id, token):
                print(f"Solicitud del taxi {taxi_id} rechazada por token inválido.")
                return

            availability = solicitud.get("availability")
            movement = solicitud.get("movement")
            localizacion = (solicitud.get("localizacion_x"), solicitud.get("localizacion_y"))

            if not localizacion:
                print(f"Error: No se recibió localización para el taxi {taxi_id}")
                return

            taxi = obtener_taxi_por_id(taxi_id)
            if taxi:
                actualizar_estado_y_posicion_taxi(
                    taxi_id,
                    availability=availability,
                    movement=movement,
                    localizacion=localizacion
                )
                if availability:
                    print(f"Taxi {taxi_id} actualizado: Disponibilidad -> {availability}")
                if movement:
                    print(f"Taxi {taxi_id} actualizado: Movimiento -> {movement}")
                print(f"Taxi {taxi_id} actualizado: Posición -> {localizacion}")
                mapa_queue.put("update")
            return

        except Exception as e:
            # La descifrado con esta clave falló, probamos con la siguiente
            continue
    
    # Si ninguna clave funcionó, hay un problema de claves
    print("No se pudo descifrar el mensaje con ninguna clave. Incidencia de comunicación por clave incorrecta.")
    # Aquí podríamos notificar al front, logs, etc.
    # Dentro del else final donde se imprime:
    # "No se pudo descifrar el mensaje con ninguna clave..."
    # No se conoce la IP real del taxi aquí, pero podrías usar la IP del broker o "Desconocido"
    log_auditoria("Desconocido", "Incidencia de cifrado", "No se pudo descifrar el mensaje: clave incorrecta")




def recibir_solicitudes_taxis():
    consumer = KafkaConsumer(
        'taxi_status',
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        auto_offset_reset='latest',
        group_id='central_taxi_status_group',
    )
    print("Central escuchando estados de taxis vía Kafka...")
    for mensaje in consumer:
        solicitud_encriptada = mensaje.value  # Esto es bytes cifrados
        manejar_solicitud_taxi(solicitud_encriptada)

# Comunicación con Clientes
def procesar_solicitudes_clientes():
    consumer = KafkaConsumer(
        'solicitudes_taxi',
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        auto_offset_reset='latest',
        group_id='central_solicitudes_group',
        value_deserializer=JsonSerializer()
    )
    print("Central escuchando solicitudes de clientes...")
    for mensaje in consumer:
        print(f"[DEBUG] Mensaje recibido en solicitudes_taxi: {mensaje.value}")
        solicitud = mensaje.value
        procesar_solicitud_cliente(solicitud)


@app.route('/api/auditoria', methods=['GET'])
def obtener_auditoria():
    auditoria_entries = []
    if os.path.exists("auditoria.log"):
        with open("auditoria.log", "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split("|")
                    if len(parts) == 4:
                        fecha_hora, ip, accion, descripcion = parts
                        auditoria_entries.append({
                            "fecha_hora": fecha_hora,
                            "ip_origen": ip,
                            "accion": accion,
                            "descripcion": descripcion
                        })

    return jsonify(auditoria_entries), 200


def procesar_solicitud_cliente(solicitud):
    cliente_id = solicitud.get("customer_id")
    destino_id = solicitud.get("destination_id")
    print(f"Solicitud recibida de cliente {cliente_id} para destino {destino_id}")

    # Obtener ciudad origen del cliente
    cliente_info = obtener_cliente_por_id(cliente_id)
    if not cliente_info:
        print(f"No se encontró información para el cliente {cliente_id}")
        # Registrar en la auditoría que no se encontró información del cliente
        log_auditoria("0.0.0.0", "Error datos cliente", f"No se encontró información para el cliente {cliente_id}")
        return

    origen = [cliente_info['localizacion_x'], cliente_info['localizacion_y']]

    # Buscar coordenadas del destino
    destino_coordenadas = obtener_coordenadas_por_id(destino_id)
    if destino_coordenadas is None:
        print(f"No se encontraron coordenadas para el destino {destino_id}")
        # Registrar en la auditoría que no se encontraron coordenadas del destino
        log_auditoria("0.0.0.0", "Error datos destino", f"No se encontraron coordenadas para el destino {destino_id}")
        return

    # Ahora, sólo si el destino existe, buscar un taxi disponible
    taxi = obtener_taxi_disponible()
    if taxi:
        taxi_id = taxi['id']
        print(f"Taxi {taxi_id} asignado a cliente {cliente_id}")
        # Registrar asignación del taxi
        log_auditoria("0.0.0.0", "Asignación de taxi", f"Cliente {cliente_id} asignado al taxi {taxi_id}")

        response = {
            "customer_id": cliente_id,
            "taxi_id": taxi_id,
            "status": "OK",
            "mensaje": f"Taxi {taxi_id} ha sido asignado y se dirige a su ubicación."
        }

        # Actualizar estado del taxi antes de enviar la solicitud
        actualizar_estado_y_posicion_taxi(taxi_id, availability="BUSY")

        # Ahora sí, enviar la solicitud al taxi
        enviar_solicitud_taxi(taxi_id, cliente_id, origen, destino_coordenadas)

        # Enviar respuesta al cliente
        enviar_notificacion_cliente(response)
    else:
        # Si no hay taxi disponible
        print(f"No hay taxis disponibles para el cliente {cliente_id}")
        # Registrar en la auditoría que no hay taxis disponibles
        log_auditoria("0.0.0.0", "Sin taxis disponibles", f"No hay taxis disponibles para el cliente {cliente_id}")

        response = {
            "customer_id": cliente_id,
            "status": "NO_TAXI_AVAILABLE",
            "mensaje": "Lo sentimos, no hay taxis disponibles en este momento."
        }
        enviar_notificacion_cliente(response)
    
socketio = SocketIO(app, cors_allowed_origins="*")

def enviar_solicitud_taxi(taxi_id, cliente_id, origen, destino):
    solicitud = {
        "taxi_id": taxi_id,
        "customer_id": cliente_id,
        "mensaje": "NUEVO_SERVICIO",
        "origen": origen,  # Debe ser una lista [x, y]
        "destino": destino  # También una lista [x, y]
    }
    try:
        producer.send('taxi_requests', value=solicitud)
        producer.flush()
        print(f"Solicitud de servicio enviada a taxi {taxi_id} para origen {origen} y destino {destino}")
    except Exception as e:
        print(f"Error al enviar solicitud de servicio a taxi {taxi_id}: {e}")

def recibir_notificaciones_viajes():
    consumer = KafkaConsumer(
        'trip_status',
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        auto_offset_reset='latest',
        group_id='central_trip_status_group',
        value_deserializer=JsonSerializer()
    )
    print("Central escuchando notificaciones de viajes...")
    for mensaje in consumer:
        data = mensaje.value
        taxi_id = str(data.get("taxi_id", ""))
        token = data.get("token")

        if not taxi_id or not token or not validar_token(taxi_id, token):
            print(
                "[WARNING] Notificación de viaje rechazada: "
                "taxi o token no válidos."
            )
            log_auditoria(
                "Desconocido",
                "Notificación de viaje rechazada",
                "Token de taxi ausente o no válido.",
            )
            continue

        if data.get("status") == "TRIP_COMPLETED":
            print(
                f"Viaje completado: taxi {taxi_id}, "
                f"cliente {data['customer_id']}."
            )
            # Actualizar la posición y estado del cliente
            actualizar_estado_cliente(
                cliente_id=data['customer_id'],
                recogido=False,
                localizacion=data['destination']
            )
            notificacion_cliente = {
                "customer_id": data["customer_id"],
                "taxi_id": taxi_id,
                "status": data["status"],
                "mensaje": data["mensaje"],
                "destination": data["destination"],
            }
            enviar_notificacion_cliente(notificacion_cliente)
            mapa_queue.put("update")

            # Registrar el evento en la auditoría
            # Usamos "0.0.0.0" como IP ya que no conocemos la IP de origen real
            # Acción: "Viaje completado", descripción: incluir el taxi_id y cliente_id
            taxi_id = data.get("taxi_id", "Desconocido")
            cliente_id = data.get("customer_id", "Desconocido")
            log_auditoria("0.0.0.0", "Viaje completado", f"Taxi {taxi_id} completó viaje para cliente {cliente_id}")

        elif data.get("status") == "PICKED_UP":
            print(
                f"Cliente {data['customer_id']} recogido "
                f"por taxi {taxi_id}."
            )
            # Establecer 'recogido' en True para el cliente
            actualizar_estado_cliente(
                cliente_id=data['customer_id'],
                recogido=True
            )
            mapa_queue.put("update")


# Función para iniciar el servidor Flask
def iniciar_api_central():
    app.run(host='0.0.0.0', port=5002, debug=False)



def enviar_notificacion_cliente(data):
    try:
        customer_id = data['customer_id']
        topic = f'respuesta_cliente_{customer_id}'  # Tópico específico para el cliente
        producer.send(topic, value=data)
        producer.flush()
        print(f"Notificación enviada al cliente {customer_id} en el tópico {topic}.")
    except Exception as e:
        print(f"Error al enviar notificación al cliente: {e}")

# MAIN
if len(sys.argv) != 4:
    print("Uso: python EC_Central.py <broker_ip> <broker_port> <central_port>")
    sys.exit(1)

broker_ip = sys.argv[1]
broker_port = int(sys.argv[2])
central_port = int(sys.argv[3])

#threading.Thread(target=iniciar_api_central, daemon=True).start()

setup_kafka(broker_ip, broker_port)

# Iniciar hilos para autenticación, recepción de mensajes de taxis y solicitudes de clientes
central_ip = '0.0.0.0'  # Escuchar en todas las interfaces
threading.Thread(target=start_taxi_auth_server,args=(central_ip, central_port),).start()
threading.Thread(target=recibir_solicitudes_taxis).start()
threading.Thread(target=procesar_solicitudes_clientes).start()
threading.Thread(target=recibir_notificaciones_viajes).start()
# Iniciar verificación en un hilo separado
threading.Thread(target=verificar_trafico, daemon=True).start()
socketio.run(app, host="0.0.0.0", port=5002)
# Ejecutar la lógica del mapa en el hilo principal
#mostrar_mapa_grafico()
