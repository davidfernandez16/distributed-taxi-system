from flask import Flask, jsonify
import requests
import threading
import time
import logging
import os
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


app = Flask(__name__)

# Configuración inicial
API_KEY = os.environ.get("OPENWEATHER_API_KEY")
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"
REQUEST_TIMEOUT = 10  # Tiempo máximo de espera para la solicitud
if not API_KEY:
    raise RuntimeError("La variable de entorno OPENWEATHER_API_KEY no está configurada. Por favor, configúrela antes de ejecutar la aplicación.")
ciudad_actual = os.environ.get("WEATHER_CITY", "Alicante")
INTERACTIVE_CITY_MODE = (
    os.environ.get("CTC_INTERACTIVE_MODE", "false").lower() == "true"
)
umbral_temperatura = 0  # Umbral para determinar viabilidad del tráfico

# Endpoint para devolver el estado del tráfico
@app.route('/traffic_status', methods=['GET'])
def verificar_estado_trafico():
    global ciudad_actual, umbral_temperatura
    try:
        respuesta = requests.get(
            WEATHER_API_URL,
            params={"q": ciudad_actual, "appid": API_KEY, "units": "metric"},
            timeout=REQUEST_TIMEOUT,
        )
        
        # Validar si la ciudad no existe
        if respuesta.status_code == 404:
            return jsonify({
                "error": "Ciudad no encontrada",
                "message": "Verifique el nombre de la ciudad",
            }), 404
        respuesta.raise_for_status()
        datos = respuesta.json()
        temperatura = datos["main"]["temp"]

        if temperatura < umbral_temperatura:
            return jsonify({
                "status": "KO", 
                "temp": temperatura, 
                "message": 
                "Tráfico no viable"}), 200
        return jsonify({
            "status": "OK",
            "temp": temperatura,
            "message": "Tráfico viable",
        }), 200

    except (requests.RequestException, ValueError, KeyError):
        return jsonify({
            "error": "Servicio meteorológico no disponible",
            "message": "No se pudo obtener el estado del tráfico. Inténtelo más tarde.",
        }), 502
    

    
def cambiar_ciudad():
    global ciudad_actual
    while True:
        nueva_ciudad = input("Introduce el nombre de la nueva ciudad: ")
        if nueva_ciudad:
            try:
                respuesta = requests.get(
                    WEATHER_API_URL,
                    params={"q": nueva_ciudad, "appid": API_KEY, "units": "metric"},
                    timeout=REQUEST_TIMEOUT,
                )

                if respuesta.status_code == 404:
                    print(f"Error: La ciudad '{nueva_ciudad}' no existe. Intente nuevamente.")
                    continue

                respuesta.raise_for_status()
                ciudad_actual = nueva_ciudad
                print(f"Ciudad cambiada a {ciudad_actual}")

            except (requests.RequestException, ValueError):
                print("No se pudo verificar la ciudad con el servicio meteorológico.")


# Ejecutar servidor Flask en un hilo aparte
def ejecutar_servidor():
    app.run(host='0.0.0.0', port=5001)

# Iniciar todo
if __name__ == "__main__":
    if INTERACTIVE_CITY_MODE:
        threading.Thread(target=ejecutar_servidor, daemon=True).start()
        cambiar_ciudad()
    else:
        ejecutar_servidor()
