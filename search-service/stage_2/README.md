# Stage 2 Microservices Scaffold

Este repositorio esqueleto separa el proyecto monolítico en **4** proyectos Maven independientes:
- `ingestion-service` (puerto 7001)
- `indexing-service`  (puerto 7002)
- `search-service`    (puerto 7003)
- `control-module`    (puerto 7004)

Cada servicio usa **Java 17 + Javalin + Gson** y expone APIs REST en JSON tal como se especifica en el enunciado.

## Endpoints (resumen)

### Ingestion Service
- `POST /ingest/{book_id}` → descarga libro (mock)
- `GET /ingest/status/{book_id}` → estado
- `GET /ingest/list` → libros disponibles

### Indexing Service
- `POST /index/update/{book_id}` → indexa/actualiza (mock)
- `POST /index/rebuild` → rehace todo (mock)
- `GET /index/status` → métricas

### Search Service
- `GET /search?q=...&author=...&language=...&year=...` → búsqueda con filtros

### Control Module
- `POST /control/run/{book_id}` → orquesta: ingestion → indexing

## Cómo compilar
```bash
# Dentro de cada servicio
mvn clean package
java -jar target/*.jar
```

## Docker Compose
```bash
docker compose build
docker compose up
```
> Ajusta las variables de entorno `INGESTION_URL` e `INDEXING_URL` del módulo `control` si quieres lanzar pipelines reales por book_id específico.

## Siguientes pasos
- Reemplazar los mocks por la lógica real de:
  - Descarga y partición header/body en datalake.
  - Construcción de índices + datamarts.
  - Acceso a índices en el buscador.
- Añadir pruebas JMH (microbenchmarks) en cada servicio.
- Añadir persistencia (ficheros/db) y logs.
