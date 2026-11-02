import json
import os

def initialize_taxis_db():
    taxis = []
    for i in range(1, 11):  # Por ejemplo, 10 taxis
        taxi = {
            "id": i,
            "availability": "BUSY",  # Todos los taxis están inicialmente ocupados
            "movement": "RUN",
            "localizacion": [0, 0]
        }
        taxis.append(taxi)

    # Guardar en archivo JSON
    with open('taxis_db.json', 'w') as file:
        json.dump(taxis, file, indent=4)

    print("Base de datos de taxis inicializada con 10 taxis en estado 'BUSY'.")

if __name__ == "__main__":
    # Verificar si el archivo ya existe
    if os.path.exists('taxis_db.json'):
        print("El archivo 'taxis_db.json' ya existe. Verifique si necesita reiniciarlo manualmente.")
    else:
        initialize_taxis_db()
