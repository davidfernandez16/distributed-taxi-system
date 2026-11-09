# EasyCab

EasyCab despliega un sistema distribuido de gestión de taxis con Docker Compose. Cada servicio representa un componente del sistema: Central, taxis, sensores, clientes, Registry, CTC, Kafka, MySQL y un mapa web en tiempo real.

Los servicios se comunican dentro de la red privada de Docker. Kafka gestiona los mensajes asíncronos, MySQL almacena el estado y Socket.IO actualiza el mapa sin recargar la página.

## Componentes

- **MySQL**: almacena taxis, clientes y localizaciones.
- **Kafka**: mensajería entre Central, taxis y clientes.
- **EC_Central**: coordina viajes, estado de taxis, tráfico y mapa.
- **EC_DE**: motor digital de cada taxi.
- **EC_S**: sensor TCP asociado a un taxi.
- **EC_Customer**: cliente que envía solicitudes de viaje.
- **EC_Registry**: registro de taxis mediante HTTPS y certificados mTLS.
- **EC_CTC**: consulta OpenWeather para determinar el estado del tráfico.
- **Frontend**: mapa y auditoría web.

## Requisitos

1. Docker Desktop instalado y en ejecución.
2. Docker Compose v2.
3. Una clave de [OpenWeather](https://openweathermap.org/api).
4. PowerShell en Windows para generar los certificados de desarrollo.

Comprueba la instalación:

```powershell
docker --version
docker compose version
```

## Configuración

Clona el repositorio y crea tu configuración local:

```powershell
git clone https://github.com/davidfernandez16/distributed-taxi-system.git
cd distributed-taxi-system
Copy-Item .env.example .env
```

Edita `.env` e indica, como mínimo:

```dotenv
OPENWEATHER_API_KEY=tu_clave_de_openweather
MYSQL_PASSWORD=una_contrasena_local
MYSQL_ROOT_PASSWORD=otra_contrasena_local
```

No subas `.env` al repositorio. Contiene datos privados y está excluido mediante `.gitignore`.

## Certificados de desarrollo

Registry y los taxis usan certificados locales. En un clon nuevo, genéralos con:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\generate-dev-certs.ps1
```

El script usa Docker para generar una CA local, el certificado de Registry, el certificado de Taxi 1 y su clave AES. No instala OpenSSL ni Python en Windows.

La carpeta `secrets/` no se sube a Git y el script no la sobrescribe si ya existe.

## Despliegue con Docker Compose

Inicia los servicios base:

```powershell
docker compose up -d --build mysql kafka ctc central registry frontend
docker compose ps
```

Abre el mapa en [http://localhost:8080](http://localhost:8080).

No se usan IPs fijas: Docker resuelve los servicios por nombre dentro de la red `easycab_network`.

## Ejecución de la simulación

Abre terminales independientes en la raíz del proyecto.

### 1. Iniciar y autenticar el taxi

```powershell
docker compose run --rm --use-aliases taxi-1
```

En el menú selecciona:

1. `1` para registrar el taxi.
2. `2` para autenticarlo en Central.

### 2. Iniciar el sensor

Con el taxi autenticado, inicia el sensor:

```powershell
docker compose run --rm --no-deps sensor-1
```

Estados disponibles:

- `o`: circulación normal.
- `r`: semáforo rojo; el taxi se detiene.
- `p`: peatón; el taxi se detiene.

### 3. Iniciar un cliente

```powershell
docker compose run --rm customer-1
```

El cliente lee sus destinos desde `EC_Request_1.json` y solicita viajes mediante Kafka. Si el único taxi está ocupado, la Central puede responder `NO_TAXI_AVAILABLE`.

## Seguridad

- Registro de taxis con HTTPS y autenticación mutua (mTLS).
- El certificado de cada taxi está vinculado a su identificador.
- Comunicación Taxi–Central cifrada con AES-GCM.
- Validación de tokens en Central; los tokens no se reenvían a clientes.
- Secretos, certificados y claves excluidos de Git.
- Errores internos de MySQL no se exponen por la API de Registry.

## Pruebas

Después de construir la imagen principal, ejecuta las pruebas con:

```powershell
docker run --rm -v "${PWD}:/app:ro" -w /app -e PYTHONDONTWRITEBYTECODE=1 easycab-app:local sh -c "pip install --no-cache-dir -r requirements-dev.txt && pytest -q -p no:cacheprovider"
```

## Estructura del proyecto

```text
.
├── EC_*.py                 # Componentes del sistema
├── EC_Request_*.json       # Solicitudes de los clientes
├── compose.yaml             # Servicios y red Docker
├── db/init/                 # Esquema inicial de MySQL
├── frontend/                # Mapa y auditoría web
├── scripts/                 # Utilidades de configuración
├── tests/                   # Pruebas automatizadas
├── .env.example             # Plantilla de configuración
└── secrets/                 # Claves locales, no versionadas
```

## Comandos útiles

Detener los servicios conservando los datos de MySQL:

```powershell
docker compose down
```

Eliminar también los datos locales y reiniciar la simulación desde cero:

```powershell
docker compose down -v
```

> `docker compose down -v` elimina la base de datos local del proyecto.

## Añadir taxis

Registrar un taxi desde su menú no requiere modificar Docker Compose. Para simular un taxi adicional en otro contenedor, debes añadir su servicio a `compose.yaml` y crear su certificado mTLS y clave AES correspondientes.
