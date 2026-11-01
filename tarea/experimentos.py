"""Fallas reproducibles. Nunca se borran volúmenes ajenos a este proyecto."""
from __future__ import annotations
import hashlib
import json
import subprocess
import sys
import time
from .core import *
from .pipeline import TABLERO_LUA, reset_redis, write_batch, verify

def worker(group, name, crash=False):
    cmd = [sys.executable,'-m','tarea.pipeline','consumidor','--grupo',group,'--nombre',name]
    if crash: cmd.append('--matar')
    return subprocess.Popen(cmd, cwd=ROOT)

def live_and_d1():
    r = redis_client()
    reset_redis()
    RUNTIME.mkdir(exist_ok=True)
    path = RUNTIME/'d1_lote.json'
    if path.exists(): path.unlink()
    producer = subprocess.Popen([sys.executable,'-m','tarea.pipeline','productor'],cwd=ROOT)
    processes = [producer]
    try:
        deadline = time.monotonic()+90
        while int(r.get('producidos') or 0) < 200:
            if producer.poll() is not None or time.monotonic()>deadline:
                raise RuntimeError('El productor no alcanzó 200 eventos')
            time.sleep(0.1)
        dashboard = worker('tablero','tablero-1')
        dead = worker('ingesta','ingesta-que-muere',True)
        processes += [dashboard,dead]
        code = dead.wait(timeout=90)
        if code != 1 or not path.exists():
            raise RuntimeError(f'El consumidor no murió en el punto previsto: {code}')
        pending = r.xpending(STREAM,'ingesta')
        details = r.xpending_range(STREAM,'ingesta','-','+',1000)
        messages = json.loads(path.read_text(encoding='utf-8'))
        ev = messages[0][1]
        with hbase() as conn:
            before = conn.table('lecturas').cells(key(ev),b'd:valor',versions=10,include_timestamp=True)
        recovered = worker('ingesta','ingesta-rescate')
        processes.append(recovered)
        for p in (producer,recovered,dashboard):
            if p.wait(timeout=900) != 0:
                raise RuntimeError('Falló un proceso del pipeline')
        with hbase() as conn:
            after = conn.table('lecturas').cells(key(ev),b'd:valor',versions=10,include_timestamp=True)
        def cells(rows):
            return [{'valor':v.decode(),'version_timestamp':ts} for v,ts in rows]
        evidence = {'salida_proceso':code,'xpending_tras_muerte':pending,'detalle_pendientes':details,
                    'consumidor_recuperacion':'ingesta-rescate','comando':'XAUTOCLAIM (min_idle_time=5000 ms)',
                    'lote':len(messages),'clave_muestra':key(ev).decode(),
                    'celda_antes':cells(before),'celda_despues':cells(after),
                    'xpending_final':r.xpending(STREAM,'ingesta')}
        save('d1',evidence)
        verify()
    finally:
        for p in processes:
            if p.poll() is None:
                p.terminate()
                p.wait(timeout=30)

def snapshot(r):
    stream = r.xrange(STREAM) if r.exists(STREAM) else []
    groups = r.xinfo_groups(STREAM) if r.exists(STREAM) else []
    latest = {k:r.hgetall(k) for k in sorted(r.scan_iter('ultimo:*'))}
    windows = {k:r.zrange(k,0,-1,withscores=True) for k in sorted(r.scan_iter('ventana:*'))}
    pending = {g['name']:r.xpending(STREAM,g['name']) for g in groups}
    def digest(value):
        return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()
    return {'stream_entradas':len(stream),'stream_sha256':digest(stream),
            'grupos':groups,'pendientes':pending,'ultimo_hashes':len(latest),
            'ultimo_sha256':digest(latest),'ventana_estaciones':len(windows),
            'ventana_lecturas':sum(map(len,windows.values())),'ventanas_sha256':digest(windows)}

def d2():
    import redis
    # Respaldo transitorio del estado final de P1-P6. DUMP no sale del computador.
    raw = redis.Redis.from_url(os.getenv('REDIS_URL','redis://127.0.0.1:6379/0'))
    original = [(key,raw.dump(key)) for key in raw.scan_iter()]
    events = list(source(NH,3000))
    result = {}
    try:
        for mode in ('aof','sin_persistencia'):
            extra = mode == 'sin_persistencia'
            compose('up','-d','--wait','--force-recreate','redis',extra=extra)
            reset_redis()
            r = redis_client()
            for name, base in [('d2_lecturas','lecturas'),('d2_lecturas_tipo','lecturas_tipo')]:
                SPLITS[name] = SPLITS[base]
            create_tables(('d2_lecturas','d2_lecturas_tipo'),reset=True)
            for ev in events:
                r.xadd(STREAM,ev,maxlen=MAXLEN,approximate=True)
                r.incr('producidos')
            done = r.xreadgroup('ingesta','ingesta-d2',{STREAM:'>'},count=2800)[0][1]
            with hbase() as conn:
                for batch in batches([ev for _,ev in done]):
                    write_batch(conn,batch,('d2_lecturas','d2_lecturas_tipo'))
            r.xack(STREAM,'ingesta',*[mid for mid,_ in done])
            r.xreadgroup('ingesta','ingesta-d2',{STREAM:'>'},count=100)
            rows = r.xreadgroup('tablero','tablero-d2',{STREAM:'>'},count=3000)[0][1]
            script = r.register_script(TABLERO_LUA)
            for mid,ev in rows:
                script(keys=[STREAM],args=[ev['estacion_id'],ev['tipo_sensor'],ev['valor'],
                       ev['ts'],ev['seq'],'tablero',mid])
            before = snapshot(r)
            with hbase() as conn:
                counts_before = {t:count_rows(conn.table(t)) for t in ('d2_lecturas','d2_lecturas_tipo')}
            time.sleep(1.2)  # estado asentado; el reinicio pedido es ordenado, no un corte de luz
            compose('restart','redis',extra=extra)
            compose('up','-d','--wait','redis',extra=extra)
            r = redis_client()
            after = snapshot(r)
            with hbase() as conn:
                counts = {t:count_rows(conn.table(t)) for t in ('d2_lecturas','d2_lecturas_tipo')}
            assert counts_before == counts
            result[mode] = {'antes':before,'despues':after,'hbase_filas_antes':counts_before,
                            'hbase_filas_despues':counts,
                            'eventos_producidos':3000,'eventos_confirmados_hbase':2800,
                            'pendientes_sin_put':100,'sin_entregar_a_ingesta':100,
                            'ventana_eventos_sin_confirmar':200,
                            'segundos_simulados_sin_confirmar':100}
            if mode == 'aof':
                assert before == after, 'AOF no preservó exactamente el estado'
            else:
                assert after['stream_entradas']==0 and not after['grupos'] and not after['ultimo_hashes'] and not after['ventana_lecturas']
    finally:
        # Dejar al corrector el tablero completo, los dos grupos y AOF configurado.
        compose('up','-d','--wait','--force-recreate','redis')
        raw = redis.Redis.from_url(os.getenv('REDIS_URL','redis://127.0.0.1:6379/0'))
        raw.flushdb()
        for key,dump in original:
            raw.restore(key,0,dump,replace=True)
    save('d2',result)

if __name__ == '__main__': d2()
