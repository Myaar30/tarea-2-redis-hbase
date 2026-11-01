"""Modelo, claves y acceso a servicios. El generador del Lab 06 se conserva intacto."""
from __future__ import annotations
import hashlib
import itertools
import json
import os
import statistics
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from .generador import ESTACIONES, eventos

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'resultados'
RUNTIME = ROOT / '.runtime'
TZ = timezone(timedelta(hours=-3))
T0 = datetime.fromisoformat('2026-11-02T00:00:00-03:00')
NH, NV = 864_000, 7_200
STREAM = 'sensores:t2'
MAXLEN = 20_000
STATIONS = dict(ESTACIONES)
FLOW = 'flujo_vehicular'
AIR = 'calidad_aire'
TABLES = ('lecturas', 'lecturas_tipo')
P5_TABLES = ('p5_estacion', 'p5_ts', 'p5_salt')
SPLITS = {
    'lecturas': ['CC-005', 'HUA-001', 'SPP-002'],
    'lecturas_tipo': ['flujo_vehicular', 'ocupacion_estacionamiento'],
    'p5_estacion': ['CC-005', 'HUA-001', 'SPP-002'],
    'p5_ts': [],
    'p5_salt': ['1#', '2#', '3#'],
}

def ms(dt):
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    return int(dt.timestamp() * 1000)

SPLITS['p5_ts'] = [str(ms(T0 + timedelta(days=d))) for d in (1, 3, 5)]

def iso(ts):
    return datetime.fromtimestamp(int(ts) / 1000, TZ).isoformat(timespec='milliseconds')

def normalize(event, seq):
    return {'estacion_id': event['estacion_id'], 'tipo_sensor': event['tipo_sensor'],
            'valor': str(event['valor']), 'ts': str(ms(event['timestamp'])), 'seq': str(seq)}

def source(start=0, count=NH+NV):
    for seq, ev in enumerate(itertools.islice(eventos(T0, semilla=2026), start+count)):
        if seq >= start:
            yield normalize(ev, seq)

def salt(est):
    # hash() de Python cambia entre procesos: SHA-256 es un hash estable.
    return int.from_bytes(hashlib.sha256(est.encode()).digest()[:8], 'big') % 4

def key(ev, table='lecturas'):
    if table.startswith('d2_'):
        table = table[3:]
    est, ts = ev['estacion_id'], f"{int(ev['ts']):013d}"
    if table in ('lecturas', 'p5_estacion'):
        return f'{est}#{ts}'.encode()
    if table == 'lecturas_tipo':
        return f"{ev['tipo_sensor']}#{ts}#{est}".encode()
    if table == 'p5_ts':
        return f'{ts}#{est}'.encode()
    if table == 'p5_salt':
        return f'{salt(est)}#{ts}#{est}'.encode()
    raise ValueError(table)

def columns(ev):
    return {f'd:{k}'.encode(): str(v).encode() for k, v in ev.items()}

def decode(row):
    return {k.decode().split(':', 1)[1]: v.decode() for k, v in row.items()}

def redis_client():
    import redis
    return redis.Redis.from_url(os.getenv('REDIS_URL', 'redis://127.0.0.1:6379/0'),
                                decode_responses=True, socket_timeout=10)

def hbase():
    import happybase
    class ManagedConnection(happybase.Connection):
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            self.close()
    return ManagedConnection(os.getenv('HBASE_HOST', '127.0.0.1'),
                             port=int(os.getenv('HBASE_PORT', '9090')), timeout=120_000)

def compose(*args, input=None, capture=True, extra=False):
    cmd = ['docker', 'compose', '-f', 'docker-compose.yml']
    if extra:
        cmd += ['-f', 'compose.sin-persistencia.yml']
    return subprocess.run(cmd + list(args), input=input, text=True, cwd=ROOT,
                          check=True, capture_output=capture)

def shell(commands):
    return compose('exec', '-T', 'hbase', 'hbase', 'shell', '-n', input=commands+'\n').stdout

def create_tables(names, reset=False):
    with hbase() as conn:
        existing = {t.decode() for t in conn.tables()}
        for name in names:
            if name in existing and not reset:
                continue
            if name in existing:
                conn.delete_table(name, disable=True)
            # Thrift no permite SPLITS al crear: usamos el shell original del lab.
            cuts = ', '.join(repr(x) for x in SPLITS[name])
            result = shell(f"create '{name}', {{NAME => 'd', VERSIONS => 1}}, {{SPLITS => [{cuts}]}}")
            if name.encode() not in conn.tables():
                raise RuntimeError(result)

def batches(items, size=1000):
    iterator = iter(items)
    while batch := list(itertools.islice(iterator, size)):
        yield batch

def write_batch(conn, events, names=TABLES, protect=True):
    for name in names:
        canonical = {}
        for ev in events:
            row = key(ev, name)
            if row not in canonical or int(ev['seq']) > int(canonical[row]['seq']):
                canonical[row] = ev
        existing = dict(conn.table(name).rows(list(canonical), columns=[b'd:seq'])) if protect else {}
        # HappyBase cuenta celdas, no filas: cinco columnas por evento.
        with conn.table(name).batch(batch_size=5000, wal=True) as batch:
            for row, ev in canonical.items():
                # Un escritor de ingesta a la vez. Se omiten replays ya durables y
                # secuencias antiguas; la segunda tabla se repara independientemente.
                old = existing.get(row, {}).get(b'd:seq', b'-1')
                if int(ev['seq']) > int(old):
                    batch.put(row, columns(ev))

def scan_events(table, **kwargs):
    for _, row in table.scan(batch_size=4000, **kwargs):
        yield decode(row)

def count_rows(table):
    return sum(1 for _ in table.scan(filter='FirstKeyOnlyFilter() AND KeyOnlyFilter()', batch_size=8000))

def save(name, value):
    OUT.mkdir(exist_ok=True)
    (OUT / f'{name}.json').write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'Resultado guardado: resultados/{name}.json', flush=True)

def load(name):
    return json.loads((OUT / f'{name}.json').read_text(encoding='utf-8'))

def measure(fn):
    samples = []
    values = []
    for _ in range(6):
        begin = time.perf_counter()
        values.append(fn())
        samples.append((time.perf_counter()-begin)*1000)
    return {'resultado': values[-1], 'calentamiento_ms': samples[0],
            'corridas_ms': samples[1:], 'mediana_ms': statistics.median(samples[1:])}
