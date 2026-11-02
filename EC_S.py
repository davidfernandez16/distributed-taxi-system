import socket
import time
import threading
import sys


# Estado global del sensor
current_status = 'o'  # Siempre comienza con 'o' (OK)
status_lock = threading.Lock()  # Lock para asegurar la sincronización


def simulate_sensor():
   """Simula el estado del sensor enviando mensajes de estado cada segundo."""
   global current_status
   while True:
       time.sleep(1)  # Enviar cada segundo el estado actual del sensor
       with status_lock:  # Bloquear el acceso a current_status mientras se envía
           yield current_status


def change_sensor_status():
   """Permite al usuario introducir manualmente el estado del sensor."""
   global current_status
   while True:
       # Esperar al input del usuario para cambiar el estado
       new_status = input("Introduce nuevo estado del sensor ('o' para OK, 'r' para semáforo rojo, 'p' para peatón): ").strip()
       print(f"Estado ingresado: '{new_status}'")  # Ver qué se está capturando
       if new_status in ['o', 'r', 'p']:  # Comparar solo con valores limpios
           with status_lock:  # Bloquear el acceso a current_status mientras se cambia
               current_status = new_status
           print(f"Estado del sensor cambiado a: {current_status}")
       else:
           print("Estado no válido. Introduce 'o', 'r' o 'p'.")


def connect_to_engine(engine_ip, engine_port):
    """Conectar el sensor al Digital Engine y enviar mensajes de estado."""
    sensor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        print(f"Intentando conectar con {engine_ip}:{engine_port}...")
        sensor.connect((engine_ip, engine_port))
        print(f"Conexión establecida con {engine_ip}:{engine_port}")
    except Exception as e:
        print(f"Error al conectar el sensor: {e}")
        return
    try:
        sensor_gen = simulate_sensor()  # Generador para el estado del sensor
        while True:
            # Obtener el siguiente estado del sensor y enviarlo al Digital Engine
            sensor_status = next(sensor_gen)
            sensor.send(sensor_status.encode())
            print(f"Sensor envía estado: {sensor_status}")
    except Exception as e:
        print(f"Error en el sensor: {e}")
    finally:
        sensor.close()



if __name__ == "__main__":
   # Verificar que se pasaron los argumentos requeridos
   if len(sys.argv) != 3:
       print("Uso: python EC_S.py <engine_ip> <engine_port>")
       sys.exit(1)


   # Leer los argumentos de línea de comandos
   engine_ip = sys.argv[1]       # IP del Digital Engine (EC_DE)
   engine_port = int(sys.argv[2])  # Puerto del Digital Engine (EC_DE)


   # Iniciar el hilo para cambiar manualmente el estado del sensor
   input_thread = threading.Thread(target=change_sensor_status)
   input_thread.daemon = True  # El hilo se cerrará automáticamente cuando el programa principal termine
   input_thread.start()


   # Conectar el sensor al Digital Engine y comenzar a enviar datos
   connect_to_engine(engine_ip, engine_port)