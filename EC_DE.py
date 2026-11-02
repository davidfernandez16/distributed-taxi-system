import socket
import threading
import sys
import time
from kafka import KafkaProducer, KafkaConsumer
import json
import queue
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from crypto_utils import encrypt_message, decrypt_message

# Variables globales
taxi_id = ""
central_ip = ""
central_port = 0
sensor_port = 0
broker_ip = ""
broker_port = 0
registry_ip = ""
registry_port = 0
token_taxi = None

authenticated = False  # Estado de autenticación
registered = False  # Estado de registro
producer = None
consumer = None
availability_status = "BUSY"  # Estado de disponibilidad del taxi (AVAILABLE/BUSY)
current_position = [0, 0]  # Posición inicial del taxi
request_queue = queue.Queue()  # Cola para manejar solicitudes de servicio
movement_event = threading.Event()  # Evento para controlar el movimiento
movement_event.set()  # Inicialmente, el taxi puede moverse
TAXI_KEY = None  # Clave del taxi

def cargar_clave_taxi():
    global TAXI_KEY
    key_file = f"clave_taxi_{taxi_id}.key"
    with open(key_file, "rb") as f:  # Modo binario
        TAXI_KEY = f.read().strip()

    # Verificar tamaño y tipo
    if len(TAXI_KEY) not in (16, 24, 32):
        print("Error: La clave no tiene un tamaño válido para AES.")
        sys.exit(1)

    # Aquí imprimes el tipo y la longitud antes de usar la clave
    print(f"Tipo de TAXI_KEY: {type(TAXI_KEY)} - Longitud: {len(TAXI_KEY)}")

    if not isinstance(TAXI_KEY, bytes):
        print("Error: La clave no es de tipo bytes.")
        sys.exit(1)

def registrar_taxi():
    global taxi_id, registered
    url = f"https://{registry_ip}:{registry_port}/register_taxi"
    data = {
        "taxi_id": taxi_id
    }
    try:
        # Cargar los certificados del taxi
        cert = ('certs/taxi_cert.pem', 'certs/taxi_key.pem')
        # Deshabilitar la verificación SSL (solo para pruebas)
        response = requests.post(url, json=data, cert=cert, verify=False)
        if response.status_code == 201:
            print(f"Taxi {taxi_id} registrado con éxito en EC_Registry.")
            registered = True
        else:
            # Manejo de errores
            try:
                error_msg = response.json().get('error')
                print(f"Error al registrar taxi: {error_msg}")
            except json.JSONDecodeError as e:
                print(f"Error al decodificar el JSON de la respuesta: {e}")
                print(f"Contenido de la respuesta: {response.text}")
    except Exception as e:
        print(f"Error de conexión con EC_Registry: {e}")


def autenticar_taxi_con_central():
    global authenticated, availability_status, token_taxi
    if not registered:
        print("Debe registrar el taxi antes de autenticarse.")
        return
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((central_ip, central_port))
            mensaje_autenticacion = json.dumps({
                "taxi_id": taxi_id,
                "position": current_position,
                "movement": "RUN" if movement_event.is_set() else "STOP"
            })
            s.send(mensaje_autenticacion.encode())
            response = s.recv(1024).decode().strip()
            print(f"Respuesta de la central: {response}")
            if response.startswith("OK#"):
                parts = response.split("#")
                # parts = ["OK", "Taxi autenticado y listo", "Token:<token>"]
                if len(parts) > 2 and "Token:" in parts[2]:
                    token_part = parts[2]
                    token_taxi = token_part.split("Token:")[1]
                    token_taxi = token_taxi.strip()
                authenticated = True
                availability_status = "AVAILABLE"
                enviar_estado()
                print(f"Taxi {taxi_id} autenticado con éxito y token recibido: {token_taxi}")
                iniciar_hilos_taxi()
            else:
                print(f"Error en la autenticación del taxi {taxi_id}: {response}")
    except Exception as e:
        print(f"Error durante la autenticación: {e}")


def dar_de_baja_taxi():
    global taxi_id, registered, authenticated
    url = f"https://{registry_ip}:{registry_port}/deregister_taxi/{taxi_id}"
    try:
        cert = ('certs/taxi_cert.pem', 'certs/taxi_key.pem')
        response = requests.delete(url, cert=cert, verify=False)
        if response.status_code == 200:
            print(f"Taxi {taxi_id} dado de baja exitosamente en EC_Registry.")
            registered = False
            authenticated = False
        else:
            print(f"Error al dar de baja el taxi: {response.json().get('error')}")
    except Exception as e:
        print(f"Error de conexión con EC_Registry: {e}")

def setup_kafka(broker_ip, broker_port):
    global producer, consumer
    producer = KafkaProducer(
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        value_serializer=lambda v: v,  # Enviaremos bytes cifrados, no JSON directo
        retries=5,
        acks='all'
    )
    consumer = KafkaConsumer(
        'taxi_requests',
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        auto_offset_reset='latest',
        group_id=f'taxi_group_{taxi_id}',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )


def iniciar_hilos_taxi():
    threading.Thread(target=recibir_datos_sensor, daemon=True).start()
    threading.Thread(target=recibir_solicitudes_kafka, daemon=True).start()
    threading.Thread(target=procesar_solicitudes, daemon=True).start()


def enviar_estado():
    global TAXI_KEY
    message = {
        "taxi_id": taxi_id,
        "availability": availability_status,
        "movement": "RUN" if movement_event.is_set() else "STOP",
        "localizacion_x": current_position[0],
        "localizacion_y": current_position[1],
        "token": token_taxi
    }
    message_bytes = json.dumps(message).encode('utf-8')
    # Cifrar con la clave del taxi
    encrypted = encrypt_message(message_bytes, TAXI_KEY)
    print(f"Enviando estado cifrado a Kafka para el taxi {taxi_id}")
    try:
        producer.send('taxi_status', value=encrypted)
        producer.flush()
        print("Estado cifrado enviado correctamente")
    except Exception as e:
        print(f"Error al enviar estado a Kafka: {e}")


def recibir_solicitudes_kafka():
    if authenticated:
        print(f"Taxi {taxi_id} esperando solicitudes de servicio vía Kafka...")
        try:
            time.sleep(2)
            for mensaje in consumer:
                solicitud = mensaje.value
                if str(solicitud.get("taxi_id")) == taxi_id:
                    request_queue.put(solicitud)
        except Exception as e:
            print(f"Error al recibir mensajes de Kafka: {e}")

def procesar_solicitudes():
    while True:
        solicitud = request_queue.get()
        manejar_solicitud(solicitud)
        request_queue.task_done()

def manejar_solicitud(solicitud):
    global availability_status, authenticated, token_taxi
    mensaje = solicitud.get("mensaje")

    if mensaje == "NUEVO_SERVICIO":
        origen = solicitud.get("origen")
        destino = solicitud.get("destino")
        cliente_id = solicitud.get("customer_id")
        print(f"Taxi {taxi_id} recibió solicitud para recoger en {origen} y llevar al destino {destino}")
        availability_status = "BUSY"
        enviar_estado()
        realizar_viaje(origen, destino, cliente_id)

    elif mensaje == "VOLVER_BASE":
        base = solicitud.get("base", [0, 0])
        print(f"Taxi {taxi_id} recibió orden de regresar a la base {base}")
        availability_status = "BUSY"
        enviar_estado()
        mover_taxi(base)
        print(f"Taxi {taxi_id} ha llegado a la base y está en estado BUSY.")
        enviar_estado()

        # Aquí se "resetea" el taxi: se elimina el token y se desautentica.
        token_taxi = None
        authenticated = False
        availability_status = "BUSY"
        enviar_estado()
        print("El token ha sido invalidado tras volver a la base. El taxi debe autenticarse de nuevo.")

    elif mensaje == "REANUDAR":
        print(f"Taxi {taxi_id} recibió orden de reanudar operaciones.")
        availability_status = "AVAILABLE"
        enviar_estado()



def mover_taxi(destino):
    global movement_event, authenticated, token_taxi
    destino_coords = destino
    print(f"Iniciando movimiento hacia {destino_coords}")

    # Comprobar si el taxi sigue autenticado y tiene token
    # Si no, no permitir que se mueva.
    if not authenticated or token_taxi is None:
        print("El taxi no tiene un token válido, no puede moverse.")
        return

    while current_position != destino_coords:
        # Verificar en cada iteración si el taxi sigue con token
        # Si en mitad del movimiento el token desaparece, dejar de moverse.
        if not authenticated or token_taxi is None:
            print("El taxi ha perdido el token durante el trayecto. Deteniendo movimiento.")
            return

        if not movement_event.is_set():
            print(f"Taxi {taxi_id} detenido en la posición: {current_position} debido a movement_status = STOP")
            movement_event.wait()
            print(f"Taxi {taxi_id} reanudando movimiento.")

        if current_position[0] < destino_coords[0]:
            current_position[0] = (current_position[0] + 1) % 20
        elif current_position[0] > destino_coords[0]:
            current_position[0] = (current_position[0] - 1) % 20

        if current_position[1] < destino_coords[1]:
            current_position[1] = (current_position[1] + 1) % 20
        elif current_position[1] > destino_coords[1]:
            current_position[1] = (current_position[1] - 1) % 20

        enviar_estado()
        print(f"Taxi {taxi_id} en la posición: {current_position} con movement_status = {'RUN' if movement_event.is_set() else 'STOP'}")
        sys.stdout.flush()
        time.sleep(1)


def realizar_viaje(origen, destino, cliente_id):
    global availability_status
    print(f"Taxi {taxi_id} comenzando viaje hacia origen {origen}...")
    mover_taxi(origen)
    print(f"Taxi {taxi_id} ha llegado a la posición de origen {origen}.")

    notify_pickup(cliente_id)

    print(f"Taxi {taxi_id} comenzando viaje hacia el destino {destino}...")
    mover_taxi(destino)
    print(f"Taxi {taxi_id} ha llegado al destino {destino}.")

    notify_trip_completion(cliente_id, destino)

    print(f"Taxi {taxi_id} regresando a la posición inicial (0, 0)...")
    mover_taxi([0, 0])

    availability_status = "AVAILABLE"
    enviar_estado()

def notify_pickup(cliente_id):
    global TAXI_KEY
    message = {
        "customer_id": cliente_id,
        "taxi_id": taxi_id,
        "status": "PICKED_UP",
        "mensaje": f"Taxi {taxi_id} ha recogido al cliente {cliente_id}.",
        "token": token_taxi
    }
    # Notificamos viaje al trip_status en claro, ya que central se asume confía en canal interno
    # Si quieres cifrar también trip_status, aplica el mismo proceso
    producer.send('trip_status', value=json.dumps(message).encode('utf-8'))
    producer.flush()
    print(f"Notificado a central que el cliente {cliente_id} ha sido recogido.")

def notify_trip_completion(cliente_id, destination):
    message = {
        "customer_id": cliente_id,
        "taxi_id": taxi_id,
        "status": "TRIP_COMPLETED",
        "mensaje": f"Taxi {taxi_id} ha completado el viaje para el cliente {cliente_id}.",
        "destination": destination,
        "token": token_taxi
   }
    try:
        producer.send('trip_status', value=json.dumps(message).encode('utf-8'))
        producer.flush()
        print(f"Notificado a central que el viaje para el cliente {cliente_id} ha sido completado.")
    except Exception as e:
        print(f"Error al notificar finalización de viaje: {e}")

def recibir_datos_sensor():
    global movement_event
    previous_status = None
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('0.0.0.0', sensor_port))
            s.listen(1)
            print(f"Taxi está escuchando conexiones del sensor en el puerto {sensor_port}...")
            while True:
                conn, addr = s.accept()
                with conn:
                    print(f"Conexión establecida con el sensor: {addr}")
                    while True:
                        data = conn.recv(1024).decode().strip()
                        if not data:
                            break

                        if previous_status is None or data != previous_status:
                            print(f"Datos recibidos del sensor: {data}")
                            previous_status = data

                        if data in ["KO", "r", "p"]:
                            if movement_event.is_set():
                                movement_event.clear()
                                enviar_estado()
                                print(f"Estado de movimiento cambiado a STOP y enviado a la central.")
                        elif data == "o":
                            if not movement_event.is_set():
                                movement_event.set()
                                enviar_estado()
                                print(f"Estado de movimiento cambiado a RUN y enviado a la central.")

        except Exception as e:
            print(f"Error al vincular el sensor en puerto {sensor_port}: {e}")

def mostrar_menu():
    while True:
        print("\n--- Menú del Taxi ---")
        print("1. Registrar Taxi")
        print("2. Autenticar Taxi")
        print("3. Dar de Baja Taxi")
        print("4. Salir")
        opcion = input("Seleccione una opción: ")

        if opcion == '1':
            registrar_taxi()
        elif opcion == '2':
            if not registered:
                print("Debe registrar el taxi antes de autenticarse.")
            else:
                autenticar_taxi_con_central()
        elif opcion == '3':
            dar_de_baja_taxi()
        elif opcion == '4':
            print("Saliendo del programa.")
            sys.exit(0)
        else:
            print("Opción no válida. Intente nuevamente.")

def main():
    if len(sys.argv) != 9:
        print(f"Uso: python EC_DE.py <taxi_id> <central_ip> <central_port> <broker_ip> <broker_port> <sensor_port> <registry_ip> <registry_port>")
        sys.exit(1)

    global taxi_id, central_ip, central_port, broker_ip, broker_port, sensor_port, registry_ip, registry_port

    taxi_id = sys.argv[1]
    central_ip = sys.argv[2]
    central_port = int(sys.argv[3])
    broker_ip = sys.argv[4]
    broker_port = int(sys.argv[5])
    sensor_port = int(sys.argv[6])
    registry_ip = sys.argv[7]
    registry_port = int(sys.argv[8])

    cargar_clave_taxi()
    setup_kafka(broker_ip, broker_port)
    mostrar_menu()

if __name__ == "__main__":
    main()
