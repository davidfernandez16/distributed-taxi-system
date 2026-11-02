import json

# Archivo de la base de datos de clientes
CLIENTES_DB_FILE = 'clientes_db.json'

# Función para guardar la base de datos de clientes
def guardar_clientes_db(clientes):
    with open(CLIENTES_DB_FILE, 'w') as file:
        json.dump(clientes, file, indent=4)

# Script para inicializar la base de datos de clientes
def initialize_clientes_db():
    clientes = {
        "1": {"localizacion": [7, 3], "recogido": False, "destination": None},
        "2": {"localizacion": [9, 14], "recogido": False, "destination": None},
        "3": {"localizacion": [13, 7], "recogido": False, "destination": None},
        "4": {"localizacion": [4, 15], "recogido": False, "destination": None},
        "5": {"localizacion": [16, 5], "recogido": False, "destination": None}
        
    }
    guardar_clientes_db(clientes)
    print("Base de datos de clientes inicializada con 5 clientes.")

if __name__ == "__main__":
    initialize_clientes_db()