import json
import os

def generar_ec_request(cliente_id, destinos):
    """
    Genera un archivo EC_Request.json para un cliente específico.

    Args:
    - cliente_id (str): ID del cliente para el que se va a generar el archivo.
    - destinos (list of str): Lista de IDs de los destinos a incluir en el archivo.

    Example:
    generar_ec_request("1", ["A", "B", "C", "D", "E"])
    """
    # Crear la estructura de datos para el archivo JSON
    request_data = {
        "Requests": {str(index): {"Id": destino} for index, destino in enumerate(destinos)}
    }

    # Nombre del archivo basado en el cliente_id
    file_name = f"EC_Request_{cliente_id}.json"

    # Guardar el archivo JSON
    with open(file_name, 'w') as json_file:
        json.dump(request_data, json_file, indent=4)
    
    print(f"Archivo '{file_name}' generado exitosamente con las siguientes solicitudes:")
    print(json.dumps(request_data, indent=4))

# Crear destinos para 10 clientes
if __name__ == "__main__":
    destinos_list = [["A", "C", "F", "D", "E"]] * 5  # Mismos destinos para simplificar
    for i in range(1, 6):
        generar_ec_request(str(i), destinos_list[i-1])