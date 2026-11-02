from flask import Flask, jsonify
import requests
import threading
import time
import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


app = Flask(__name__)

# Configuración inicial
API_KEY = "os.environ.get("OPENWEATHER_API_KEY")"  # Reemplazar con tu API Key de OpenWeather
ciudad_actual = "Alicante"  # Ciudad por defecto
umbral_temperatura = 0  # Umbral para determinar viabilidad del tráfico

# Endpoint para devolver el estado del tráfico
@app.route('/traffic_status', methods=['GET'])
def verificar_estado_trafico():
    global ciudad_actual, umbral_temperatura
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad_actual}&appid={API_KEY}&units=metric"
        respuesta = requests.get(url)
        datos = respuesta.json()
        
        # Validar si la ciudad no existe
        if datos.get("cod") == "404":
            return jsonify({"error": "Ciudad no encontrada", "message": "Verifique el nombre de la ciudad"}), 404

        temperatura = datos["main"]["temp"]

        if temperatura < umbral_temperatura:
            return jsonify({"status": "KO", "temp": temperatura, "message": "Tráfico no viable"}), 200
        else:
            return jsonify({"status": "OK", "temp": temperatura, "message": "Tráfico viable"}), 200

    except Exception as e:
        return jsonify({"error": str(e), "message": "Error al obtener el estado del tráfico"}), 500

def cambiar_ciudad():
    global ciudad_actual
    while True:
        nueva_ciudad = input("Introduce el nombre de la nueva ciudad: ")
        if nueva_ciudad:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={nueva_ciudad}&appid={API_KEY}&units=metric"
            try:
                respuesta = requests.get(url)
                datos = respuesta.json()
                if datos.get("cod") == "404":
                    print(f"Error: La ciudad '{nueva_ciudad}' no existe. Intente nuevamente.")
                else:
                    ciudad_actual = nueva_ciudad
                    print(f"Ciudad cambiada a {ciudad_actual}")
            except Exception as e:
                print(f"Error al verificar la ciudad: {e}")


# Ejecutar servidor Flask en un hilo aparte
def ejecutar_servidor():
    app.run(host='0.0.0.0', port=5001)

# Iniciar todo
if __name__ == "__main__":
    threading.Thread(target=ejecutar_servidor, daemon=True).start()
    cambiar_ciudad()
