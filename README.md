# Tarea 2 — Redis + HBase

**Procesamiento Masivo de Datos · ELE051-B / EIN102B · USM 2026-2 · Paralelo 701**  
**Integrantes:** Nicolas Galaz, Martin Yañez y Eduardo Roa

Proyecto de integración entre Redis y HBase utilizando lecturas de sensores del Gran Concepción. Redis recibe los eventos en vivo y mantiene los datos necesarios para el tablero; HBase almacena el histórico y permite consultar rangos de tiempo. También se comparan distintos diseños de claves y se prueban fallas durante el procesamiento.

El código, las mediciones y las evidencias están en este repositorio. El resumen de resultados se encuentra en [`informe.pdf`](informe.pdf).

## Ejecución

**Requisitos:** Docker con Compose v2, Python 3.11 o 3.12, conexión a Internet para instalar dependencias y construir la imagen, aproximadamente 4 GB de RAM para Docker y 3 GB de espacio libre. Deben estar disponibles los puertos `6379`, `9090` y `16010`.

Con Docker iniciado, ejecutar desde la raíz del repositorio:

**Windows (PowerShell):**

```powershell
powershell -ExecutionPolicy Bypass -File .\run.ps1
```

**Linux o macOS:**

```bash
bash run.sh
```

Los scripts preparan el entorno de Python, instalan las dependencias, levantan Redis y HBase con `docker compose up -d --build --wait` y ejecutan la carga, las consultas y las pruebas. La primera ejecución puede tardar más por la descarga de HBase. No se utiliza el VPS del curso ni se requieren credenciales.

**Nota:** Una ejecución desde cero reinicia las tablas de la tarea y la base 0 del Redis dedicado a este proyecto. Los volúmenes tienen el prefijo `tarea2-galaz-yanez-roa`; no se modifican otros proyectos de Docker.

Para repetir las consultas sobre una carga ya completada:

```bash
.venv/bin/python -m tarea.run --solo-consultas
```

En Windows, reemplazar `.venv/bin/python` por `.\.venv\Scripts\python.exe`. El modo `--reutilizar-historico` solo sirve cuando las tablas contienen las 846.906 filas del histórico, antes de cargar el tramo en vivo.

## Datos utilizados

El generador proviene del Laboratorio 06 y se ejecuta con `eventos(t0, semilla=2026)`, desde `2026-11-02T00:00:00-03:00`. Se cargan 864.000 eventos históricos directamente a HBase y después 7.200 eventos en un stream de Redis, a razón de uno cada 25 ms.

| Etapa | Eventos | Duplicados exactos | Claves distintas (estación + timestamp) |
|---|---:|---:|---:|
| Histórico | 864.000 | 17.090 | 846.906 |
| En vivo | 7.200 | 153 | 7.047 |
| **Total** | **871.200** | **17.243** | **853.953** |

La clave usa la fecha de **medición**, no la de llegada. Esto permite ubicar las lecturas tardías en el lugar correcto y reutilizar la misma fila cuando llega un duplicado. Hay cuatro colisiones entre lecturas distintas con igual estación y milisegundo, tal como indica el enunciado. Las fechas se trabajan con el offset `-03:00` y las consultas usan rangos semiabiertos `[inicio, fin)`.

## Diseño de HBase

Las tablas se crean con regiones predivididas (`SPLITS`) y una familia `d` con una versión por celda.

| Tabla | Clave de fila | División inicial | Consulta favorecida |
|---|---|---|---|
| `lecturas` | `estacion#ts` | `CC-005`, `HUA-001`, `SPP-002` | Lecturas de una estación en un intervalo |
| `lecturas_tipo` | `tipo#ts#estacion` | `flujo_vehicular`, `ocupacion_estacionamiento` | Lecturas por tipo de sensor |
| `p5_estacion` | `estacion#ts` | Igual que `lecturas` | Intervalo global con 16 rangos |
| `p5_ts` | `ts#estacion` | Cortes por día: 03, 05 y 07 de noviembre | Intervalo global con un rango |
| `p5_salt` | `salt#ts#estacion` | `1#`, `2#`, `3#` | Intervalo global con cuatro rangos |

`lecturas` distribuye las 16 estaciones en cuatro regiones. `lecturas_tipo` añade una segunda escritura por evento, pero evita consultar estación por estación cuando el filtro principal es el tipo. Para P5, el salt se calcula con SHA-256 de la estación módulo 4, de manera que sea estable entre ejecuciones.

Las inserciones se hacen por lotes. Cada fila guarda también una secuencia (`seq`) para detectar reintentos anteriores. Si una escritura queda pendiente, el consumidor puede repetirla sin generar filas adicionales. El diseño utiliza un escritor de ingesta activo; no implementa transacciones distribuidas ni escrituras concurrentes sobre una misma fila.

## Procesamiento en Redis

El productor publica en `sensores:t2` y dos grupos independientes consumen el mismo stream:

- **`ingesta`:** escribe cada lote en `lecturas` y `lecturas_tipo`; confirma con `XACK` después de escribir en HBase.
- **`tablero`:** actualiza las estructuras del estado actual y luego confirma su procesamiento.

| Clave | Estructura | Contenido |
|---|---|---|
| `ultimo:<estacion>` | Hash | Último valor, timestamp de medición, tipo y secuencia |
| `ranking:flujo` | Sorted set | Estaciones de flujo ordenadas por su último valor |
| `ventana:<estacion>` | Sorted set | Lecturas de flujo de los últimos 10 minutos de medición |
| `tipo:<tipo>` | Set | Catálogo de estaciones por tipo |

El último valor se determina comparando `(timestamp de medición, seq)`, para que una lectura tardía no reemplace una más reciente. Las ventanas se recortan respecto al timestamp máximo observado en el stream. Se utiliza un hash auxiliar `ventana_seq:<estacion>` para resolver reintentos y colisiones.

Redis está configurado con **AOF (`appendfsync everysec`)**, volumen persistente y snapshots RDB automáticos deshabilitados. El stream utiliza `MAXLEN ~ 20000`: a 40 eventos por segundo equivale a unos 500 segundos de producción y alcanza para conservar los 7.200 eventos de esta prueba. El límite es aproximado y no garantiza que un consumidor atrasado alcance a leer antes de un recorte.

## Consultas y mediciones

Se resolvieron las seis preguntas de la tarea:

1. **P1:** último valor de cada estación y top 5 de flujo, comparando Redis y HBase. Se identifica también la diferencia entre última medición y última llegada.
2. **P2:** promedio de flujo de los últimos 10 minutos desde los sorted sets de Redis, con medición de memoria.
3. **P3:** lecturas de `CC-001` entre las 08:00 y las 10:00 del 4 de noviembre, comparando scan por rango y recorrido completo.
4. **P4:** promedio y máximo de PM2.5 por estación, comparando acceso por estación con acceso por tipo de sensor.
5. **P5:** distribución de escrituras por región mediante JMX y tiempos de consulta para las tres estrategias de claves.
6. **P6:** comuna con mayor diferencia de flujo entre sus estaciones en el momento actual y horario del pico agregado del día anterior, combinando Redis y HBase.

Cada consulta se ejecuta una vez para calentar cachés y cinco veces para medir. Se reporta la **mediana de las cinco mediciones**, junto con los tiempos individuales. Los JSON de `resultados/` contienen las salidas y las mediciones; las características del computador están en `resultados/equipo.json`.

## Pruebas de falla

**D1 — Falla del consumidor.** El consumidor de ingesta termina con `os._exit(1)` después de escribir un lote en HBase y antes de ejecutar `XACK`. Se revisa `XPENDING` y un segundo consumidor recupera las entradas mediante `XAUTOCLAIM`. El resultado final vuelve a verificarse contra las 853.953 filas de cada tabla. La evidencia está en `resultados/d1.json`.

**D2 — Reinicio de Redis.** Se comparan dos configuraciones después de producir 3.000 eventos: AOF con volumen y Redis sin persistencia. Se comprueba qué ocurre con el stream, los grupos, las entradas pendientes y el tablero. Se usan tablas de prueba separadas para no alterar el histórico final. Los detalles están en `resultados/d2.json`.

El histórico confirmado en HBase permanece aunque Redis se reinicie. En cambio, los eventos todavía no escritos en HBase dependen de la persistencia del stream. `appendfsync everysec` reduce esa exposición, pero no elimina por completo la posible pérdida ante un fallo brusco.

## Comprobaciones

Después de ejecutar el proyecto se puede verificar el estado con:

```bash
.venv/bin/python verificar.py
.venv/bin/python -m unittest discover -s tests -v
docker compose exec redis redis-cli XPENDING sensores:t2 ingesta
docker compose exec redis redis-cli XPENDING sensores:t2 tablero
docker compose exec redis redis-cli HGETALL ultimo:CC-001
docker compose exec redis redis-cli ZREVRANGE ranking:flujo 0 4 WITHSCORES
```

En Windows se utiliza `\.venv\Scripts\python.exe` en los dos primeros comandos. Los pendientes y el lag de ambos grupos deben terminar en cero.

## Archivos principales

| Ruta | Contenido |
|---|---|
| `docker-compose.yml`, `compose.sin-persistencia.yml` | Infraestructura y configuración alternativa de Redis |
| `hbase/` | Dockerfile de HBase y versión original del Lab 07 |
| `tarea/generador.py` | Generador de eventos del Lab 06 |
| `tarea/core.py`, `tarea/pipeline.py` | Claves, carga histórica y consumidores |
| `tarea/consultas.py`, `tarea/experimentos.py` | Consultas P1–P6 y fallas D1–D2 |
| `tarea/informe.py` | Generación del informe desde los resultados |
| `resultados/` | Tiempos, conteos y evidencias de ejecución |
| `verificar.py`, `tests/` | Verificación final y pruebas |

Los datos generados, `.venv/`, `.runtime/`, volúmenes y archivos temporales están excluidos mediante `.gitignore`.

## Problemas encontrados y limitaciones

Durante la construcción de HBase, la URL principal de Apache respondió 404 y una fuente alternativa descargaba demasiado lento. Se incorporó un espejo y se mantuvo la verificación SHA-512 del archivo. También fue necesario crear los `SPLITS` desde HBase shell, ya que HappyBase no permite configurarlos al crear las tablas. Para el salting se evitó el hash interno de Python, cuyo resultado no es estable entre procesos.

Las pruebas se ejecutaron sobre **HBase standalone**, en un solo equipo. Por eso, la distribución de escrituras ayuda a estudiar el efecto de las claves, pero los tiempos no representan el comportamiento de un clúster con varios RegionServers. Con más consumidores de ingesta sería necesario incorporar una actualización condicional atómica por fila; además, en un sistema real se debería controlar el lag y aplicar backpressure antes de recortar eventos del stream.

## Referencias

- [Repositorio del curso (laboratorios 06 y 07)](https://github.com/diegoalrv/ein102b)
- [Redis: XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/), [consumer groups](https://redis.io/docs/latest/commands/xreadgroup/) y [persistencia](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Apache HBase Reference Guide](https://hbase.apache.org/book.html)
- [HappyBase API](https://happybase.readthedocs.io/en/latest/api.html)

<!-- RESULTADOS_MEDIDOS -->

## Ejecución verificada

Tiempo del pipeline, consultas y pruebas: **851,8 s**, sin descarga inicial. Carga histórica: **129,8 s** (aprox. **6.657 eventos/s**). Verificación final: **853.953 filas por tabla**, **7.200 entradas procesadas por grupo**, con pendientes y lag en cero.
