# Tarea 2 Redis y HBase

Integrantes: **Nicolas Galaz, Martin Yañez y Eduardo Roa**. Procesamiento Masivo de Datos, ELE051-B / EIN102B, USM 2026-2, paralelo 701.

La entrega incluye la infraestructura, la carga histórica, el productor, dos grupos independientes, las seis preguntas, las dos pruebas de falla y un informe PDF de hasta cinco páginas. Los resultados no son números escritos a mano: cada corrida guarda sus tiempos y evidencias en `resultados/` y genera `informe.pdf` sólo si la verificación final pasa.

## Reproducir desde cero

Se necesita Docker con Compose v2 o posterior, Python 3.11 o 3.12 con venv, conexión a Internet para instalar dependencias y construir la imagen, unos 4 GB de RAM disponibles para Docker y al menos 3 GB de disco. Los puertos locales 6379, 9090 y 16010 deben estar libres. No usamos el VPS del curso. No se necesitan credenciales ni archivos `.env`.

En Linux o macOS, desde la raíz del repositorio:

```bash
bash run.sh
```

En Windows, con Docker Desktop iniciado y PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\run.ps1
```

Ese único comando crea el ambiente virtual, instala `requirements.txt`, ejecuta `docker compose up -d --build --wait` y corre toda la entrega. **La ejecución desde cero reinicia exclusivamente las tablas y la base Redis de este proyecto dedicado.** No se debe compartir su DB 0 con otra aplicación. Los volúmenes del proyecto tienen el prefijo `tarea2-galaz-yanez-roa`; no se tocan otros proyectos de Docker.

La primera construcción descarga HBase (~329 MiB) y puede tardar varios minutos según la red. La hora en vivo tarda aproximadamente tres minutos, además de la carga histórica, los seis recorridos completos de P3 y las pruebas de reinicio. La duración real y el equipo quedan al final de este README y en `resultados/ejecucion.json` y `resultados/equipo.json`.

Para repetir sólo las consultas y las pruebas de D2 sobre una ejecución completa existente:

```bash
.venv/bin/python -m tarea.run --solo-consultas
```

En Windows se usa `.\.venv\Scripts\python.exe` en lugar de `.venv/bin/python`. Para desarrollo, `--reutilizar-historico` acepta únicamente tablas con 846.906 filas, antes del vivo. Si ya se cargó la hora en vivo, se debe correr desde cero. Las opciones no se necesitan para la corrección.

## Archivos y evidencia

- `docker-compose.yml`: exactamente dos servicios, Redis y HBase, con volúmenes propios y healthchecks.
- `hbase/Dockerfile`: el Dockerfile del Lab 07, HBase 2.5.15 sobre Java 11, con un espejo adicional y checksum SHA-512 oficial. El original sin cambios queda en `hbase/Dockerfile.lab07`.
- `tarea/generador.py`: copia sin cambios del Lab 06. Se invoca `eventos(t0, semilla=2026)`; no se usa su semilla por defecto.
- `tarea/core.py`: claves, SPLITS, batches y medición.
- `tarea/pipeline.py`: carga, productor, ambos consumidores y verificación.
- `tarea/consultas.py`: código de P1 a P6, resultados y tiempos.
- `tarea/experimentos.py`: muerte real con `os._exit(1)`, rescate y reinicios.
- `tarea/informe.py`: informe final construido con la evidencia.
- `verificar.py`: comprobación independiente del estado al terminar.
- `resultados/*.json`: resultados agregados, seis duraciones por consulta, métricas JMX y evidencia de fallas. Se versionan; no son el dataset.
- `.runtime/`, `.venv/`, datos, dumps y volúmenes se excluyen en `.gitignore`.

## Datos y relojes

El inicio es `2026-11-02T00:00:00-03:00`. Los primeros 864.000 eventos van directamente a HBase; los 7.200 siguientes van a `sensores:t2`, uno cada 25 ms. La auditoría genera los datos y exige los tres controles del enunciado:

| Fase | Eventos | Repetidos exactos | Claves estación y timestamp |
|---|---:|---:|---:|
| Histórico | 864.000 | 17.090 | 846.906 |
| Vivo | 7.200 | 153 | 7.047 |
| Total | 871.200 | 17.243 | 853.953 |

Los cuatro choques de igual estación y milisegundo con distinto valor explican el resto. El timestamp de la row key es siempre el de medición. Guardamos `seq`, la posición determinista del evento en la fuente, como desempate de llegada; no forma parte de la clave. Mostramos fechas en UTC-03 y usamos milisegundos desde epoch con ancho fijo de 13 dígitos en las claves. Los rangos de P3, P4 y P5 son semiabiertos `[inicio, fin)`.

## Claves y regiones

| Tabla | Row key | SPLITS | Acceso favorecido |
|---|---|---|---|
| lecturas | estación#ts | CC-005, HUA-001, SPP-002 | Un rango de una estación |
| lecturas_tipo | tipo#ts#estación | flujo_vehicular, ocupacion_estacionamiento | Un rango de un tipo |
| p5_estacion | estación#ts | Como lecturas | 16 rangos para un intervalo global |
| p5_ts | ts#estación | 03-nov, 05-nov, 07-nov a medianoche | Un rango global; escritura reciente concentrada |
| p5_salt | salt#ts#estación | 1#, 2#, 3# | Cuatro rangos globales; distribución por salt |

Lecturas reparte las 16 estaciones en cuatro grupos de cuatro; el prefijo no es la comuna, de modo que no confundimos balance de estaciones con balance geográfico. Lecturas_tipo agrupa por los tres tipos y paga una segunda copia. Los SPLITS se aplican con HBase shell porque HappyBase/Thrift no los expone al crear tablas. Las métricas JMX, las claves de cada región y el código de consultas permiten revisar el efecto real.

Usamos `int.from_bytes(SHA256(estación)[:8]) % 4`. La expresión cumple el reparto hash módulo cuatro, pero evita que el hash aleatorio de Python cambie entre procesos. La tabla de asignación se guarda en P5. Un catálogo pequeño puede quedar desbalanceado incluso con salting.

Cada tabla tiene una familia `d` y `VERSIONS = 1`. Se guardan estación, tipo, valor, timestamp de medición y secuencia. La carga consolida las claves repetidas de cada lote conservando el último evento. En vivo, antes de escribir un lote se leen las secuencias ya guardadas: el replay de una secuencia igual o menor se omite en esa tabla. Así, si falla la segunda escritura, se completa de forma independiente sin cambiar una versión ya durable.

Hay **un escritor de ingesta activo a la vez**. El rescate se inicia cuando el proceso anterior ya terminó; primero recorre pendientes con `XAUTOCLAIM`, luego lee novedades con `>`. La comparación y el put no son una transacción atómica entre consumidores concurrentes. Para varios escritores habría que agregar una operación condicional atómica por fila o particionar la propiedad de las estaciones; no atribuimos esa garantía al diseño actual. Tampoco existe una transacción entre ambas tablas: la convergencia la aporta el replay antes de confirmar.

## Redis y retención

Elegimos AOF con volumen, `appendfsync everysec` y RDB automático deshabilitado (`save ""`). Buscamos conservar el stream, los grupos, la PEL y el tablero durante reinicios sin pagar un fsync por comando. Ante una caída brusca, AOF everysec tiene una ventana aproximada de un segundo; la prueba de reinicio ordenado no mide un corte de energía. Si la pérdida aceptable fuera cero frente al proceso o al sistema, elegiríamos `always`, mediríamos su latencia y también consideraríamos la durabilidad real del almacenamiento y la alta disponibilidad.

`MAXLEN ~ 20000` supera la hora completa de 7.200 eventos. A 40 eventos/s representa aproximadamente 500 s de atraso (8 min 20 s reales, 2 h 46 min 40 s simulados). `~` hace el límite aproximado por bloques. **No es un mecanismo de protección de consumidores lentos:** una entrada recortada sin leer se pierde para ellos, y un ID pendiente cuyo cuerpo fue recortado no puede recuperarse. El consumidor rechaza IDs eliminados reportados por XAUTOCLAIM; el verificador también comprueba lag, procesadas y controles. En producción vigilaríamos el grupo más atrasado y frenaríamos al productor antes del límite; esta corrida de 7.200 entradas no alcanza el recorte.

El grupo `ingesta` escribe las dos tablas y después ejecuta XACK más incremento del contador en una operación Lua. `tablero` aplica el hash, ranking, ventana, catálogo, XACK y su contador en otra operación Lua. El contador se incrementa con el número realmente confirmado por XACK, así que un reintento de ACK no lo infla. Cada grupo recibe todo el stream.

El hash `ultimo:<estación>` guarda `valor`, `ts`, `tipo` y `seq`. Comparamos `(ts, seq)` para que una lectura tardía no haga retroceder el tablero. `ranking:flujo` sólo incluye estaciones de flujo. El catálogo son los sets `tipo:<tipo>`.

`ventana:<estación>` es un zset de flujo con score igual al timestamp de medición y miembro `ts|valor`. Se usa un hash auxiliar `ventana_seq:<estación>` para resolver colisiones al mismo timestamp y reintentos viejos. Recortamos **todas** las ventanas según el máximo timestamp global observado, incluso las de estaciones que no han recibido eventos recientes. Conservamos el intervalo cerrado `[ancla-600000, ancla]`, y eliminamos también los auxiliares vencidos.

## Las preguntas y cómo se miden

Todas las consultas ejecutan una corrida de calentamiento y **cinco adicionales**. La mediana se calcula sobre esas cinco, y se guardan los cinco tiempos crudos y el calentamiento por separado en milisegundos. La conexión se mantiene abierta donde corresponde; el tiempo incluye lectura, transferencia y agregación. No insertamos números estimados en el informe.

1. P1 consulta Redis con pipeline y HBase con 16 scans inversos por estación, límite 1. Compara todos los valores y el top 5; guarda también la estación cuya última llegada difiere de la última medición. Los hashes `llegada:*` son evidencia auxiliar de esa diferencia.
2. P2 obtiene muestras sólo de los `ventana:*`, con ZRANGEBYSCORE. MEMORY USAGE se mide con `SAMPLES 0`; la comparación de una hora sin recorte se construye en claves transitorias, que luego se eliminan. Se informa por separado la memoria de secuencias auxiliares.
3. P3 realiza un scan por rango y un recorrido de la tabla completa, filtrando estación y tiempo en Python. Ambos se repiten seis veces y deben coincidir. Agrupa en ocho cuartos de hora.
4. P4 incluye SMEMBERS del catálogo dentro de la alternativa por estación y la compara con un scan por tipo. Resuelve el máximo repetido por su primera medición. Mide espacio con `du -sb` después de flush, sobre los directorios de cada tabla: no es todo el volumen ni incluye WAL o snapshots.
5. P5 carga tres tablas nuevas con los mismos lotes de la hora en vivo. Espera 12 s para publicación de métricas JMX antes y después; resta `writeRequestCount` de cada región. El batching consolida claves repetidas y JMX puede contar menos de 7.200 mutaciones, aunque la fuente tenga 7.200 eventos. Compara 16, uno y cuatro rangos; verifica igualdad del resultado completo y guarda su hash SHA-256, sin versionar las filas de datos.
6. P6 formula una pregunta nueva: qué comuna tiene ahora mayor diferencia de flujo entre sus estaciones, y en qué intervalo de cinco minutos tuvo su pico agregado entre 00 y 01 horas del día anterior. Sólo considera comunas con al menos dos estaciones de flujo. Redis selecciona la comuna; HBase calcula la suma de promedios por estación en cada intervalo, exige muestras de todas y resuelve empates por primera hora.

HBase standalone ejecuta todas las regiones en un mismo proceso y disco local. La distribución de escrituras muestra hotspotting por clave, pero los tiempos no equivalen a un cluster con varios RegionServers. No prometemos un factor fijo de mejora al crecer a cinco años.

## Pruebas de falla

**D1.** El productor empieza a 25 ms por evento. Tras al menos 200 eventos se inicia `ingesta-que-muere`, que toma un lote, escribe ambas tablas, guarda la identificación del lote en `.runtime/` y ejecuta `os._exit(1)` antes de XACK. El coordinador exige salida 1, captura XPENDING y la muestra de versiones de una celda, y arranca `ingesta-rescate` con XAUTOCLAIM (5.000 ms). Se espera a que productor y consumidores terminen y se exigen 853.953 filas en ambas tablas, 7.200 procesadas y pendientes y lag en cero. El detalle de todos los pendientes, propietario y celda antes/después queda en `resultados/d1.json`.

**D2.** Se repite un checkpoint del pipeline con 3.000 producidos: 2.800 escritos y confirmados, 100 entregados sin put ni ACK, 100 aún no entregados; tablero procesó los 3.000. Para aislar la prueba se usan dos tablas `d2_*` que se reinician en cada configuración. No se usa la tabla final ya cargada para afirmar que esos eventos estaban protegidos. Se reinicia Redis con el compose principal AOF y con el override sin persistencia; se comparan conteos, grupos, pendientes y hashes del contenido. Los eventos del checkpoint se generan sin esperar el reloj para acelerar esta preparación; la prueba del camino en vivo B sí respeta los 25 ms. Finalmente se restaura el estado completo de Redis de la corrida principal, incluidos los grupos, con DUMP/RESTORE transitorio en memoria, y se vuelve a verificar todo.

La ventana expuesta del checkpoint son 200 **eventos sin confirmación**, no necesariamente 200 claves nuevas. La historia cargada y las escrituras ya confirmadas en HBase sobreviven independientemente de Redis. Un lote ya escrito pero no confirmado, como D1, requiere replay y no es historia perdida. Un sistema sin persistencia pierde el buffer todavía no escrito al reiniciar; AOF protege ese buffer, sujeto a su política de fsync y a las garantías del disco.

## Comprobar la entrega

```bash
.venv/bin/python verificar.py
.venv/bin/python -m unittest discover -s tests -v
docker compose exec redis redis-cli XPENDING sensores:t2 ingesta
docker compose exec redis redis-cli XPENDING sensores:t2 tablero
docker compose exec redis redis-cli HGETALL ultimo:CC-001
docker compose exec redis redis-cli ZREVRANGE ranking:flujo 0 4 WITHSCORES
```

Las pruebas de tablero usan exclusivamente Redis DB 15 y limpian esa base al terminar; no alteran DB 0. Comprueban llegada tardía, reenvío, colisión y recorte global cuando avanza otra estación. Para apagar los servicios conservando la historia: `docker compose down`.

## Qué no funcionó y qué cambiaríamos

La URL de distribución principal de Apache respondió 404 durante la construcción y el archivo alternativo descargaba demasiado lento. Añadimos un espejo y verificamos el binario con el SHA-512 oficial de Apache, fijado para HBase 2.5.15; el Dockerfile original queda como referencia. La construcción inicial depende de esa red y no se incluye como tiempo de carga de datos. El cliente HappyBase no permite SPLITS al crear tablas, por eso se utiliza HBase shell. El hash interno de Python no sirve como salt estable entre procesos, por eso usamos SHA-256. El orden de llegada tampoco sirve para el último valor; la prueba de P1 documenta la diferencia real. Para producción agregaríamos backpressure según lag, varios RegionServers, almacenamiento distribuido y control condicional de secuencia antes de ampliar la cantidad de escritores de ingesta.

## Fuentes

- Enunciado Tarea 2 Redis + HBase, del stream a la historia, USM 2026-2, profesor Diego Ramírez.
- [Repositorio del curso y código reutilizado](https://github.com/diegoalrv/ein102b): generador del Lab 06 y Dockerfile del Lab 07.
- [Redis XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/), [consumer groups](https://redis.io/docs/latest/commands/xreadgroup/) y [persistencia](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/).
- [Apache HBase Reference Guide](https://hbase.apache.org/book.html): diseño de row keys, versiones, regiones y métricas.
- [SHA-512 oficial de HBase 2.5.15](https://archive.apache.org/dist/hbase/2.5.15/hbase-2.5.15-bin.tar.gz.sha512): comprobación del binario descargado por la imagen.
- [HappyBase API](https://happybase.readthedocs.io/en/latest/api.html): batches y scans inversos.

## Entrega por Aula

Se entrega **el enlace de este repositorio Git público y `informe.pdf`**. Los resultados agregados deben quedar dentro del repositorio. Antes de enviar, abrir el enlace en una ventana sin sesión para comprobar que es público. No se incluyen `.venv`, `.runtime`, volúmenes, archivos de datos generados ni secretos.

<!-- RESULTADOS_MEDIDOS -->

## Ejecución verificada

Tiempo del pipeline, consultas y pruebas: 851.8 s, sin descarga inicial. Carga histórica: 129.8 s (6657 eventos/s). Verificación final: 853.953 filas por tabla, 7.200 entradas procesadas por grupo, pendientes y lag en cero.
