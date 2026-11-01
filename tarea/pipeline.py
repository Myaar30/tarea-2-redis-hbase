"""Carga histórica y consumidores independientes, con recuperación de la PEL."""
from __future__ import annotations
import argparse
import json
import os
import time
from .core import *

# Último por (medición, secuencia). La ventana usa tiempo de evento global;
# todos los cambios de tablero, el XACK y el contador son una sola operación.
TABLERO_LUA = r'''
local est, tipo, valor = ARGV[1], ARGV[2], ARGV[3]
local ts, seq = tonumber(ARGV[4]), tonumber(ARGV[5])
local stream, group, id = KEYS[1], ARGV[6], ARGV[7]
local watermark = math.max(tonumber(redis.call('GET','tablero:watermark') or '0'),ts)
redis.call('SET','tablero:watermark',tostring(watermark))
redis.call('SADD','tipo:'..tipo,est)
local u = 'ultimo:'..est
local oldts = tonumber(redis.call('HGET',u,'ts') or '-1')
local oldseq = tonumber(redis.call('HGET',u,'seq') or '-1')
if ts > oldts or (ts == oldts and seq >= oldseq) then
  redis.call('HSET',u,'valor',valor,'ts',ARGV[4],'tipo',tipo,'seq',ARGV[5])
  if tipo == 'flujo_vehicular' then redis.call('ZADD','ranking:flujo',valor,est) end
end
local a = 'llegada:'..est
if seq >= tonumber(redis.call('HGET',a,'seq') or '-1') then
  redis.call('HSET',a,'valor',valor,'ts',ARGV[4],'tipo',tipo,'seq',ARGV[5])
end
if tipo == 'flujo_vehicular' and ts >= watermark-600000 then
  local v, s = 'ventana:'..est, 'ventana_seq:'..est
  if seq >= tonumber(redis.call('HGET',s,ARGV[4]) or '-1') then
    redis.call('ZREMRANGEBYSCORE',v,ts,ts)
    redis.call('ZADD',v,ts,ARGV[4]..'|'..valor)
    redis.call('HSET',s,ARGV[4],ARGV[5])
  end
end
for _,station in ipairs(redis.call('SMEMBERS','tipo:flujo_vehicular')) do
  local v,s = 'ventana:'..station,'ventana_seq:'..station
  for _,member in ipairs(redis.call('ZRANGEBYSCORE',v,'-inf','('..(watermark-600000))) do
    redis.call('HDEL',s,string.match(member,'^(%d+)|'))
  end
  redis.call('ZREMRANGEBYSCORE',v,'-inf','('..(watermark-600000))
end
local ack=redis.call('XACK',stream,group,id)
redis.call('INCRBY','procesadas:'..group,ack)
return ack
'''
ACK_LUA = "local n=redis.call('XACK',KEYS[1],ARGV[1],unpack(ARGV,2)); redis.call('INCRBY','procesadas:'..ARGV[1],n); return n"

def reset_redis():
    r = redis_client()
    # La base y el proyecto de compose están dedicados a esta entrega.
    r.flushdb()
    for est, tipo in STATIONS.items():
        r.sadd('tipo:'+tipo, est)
    for group in ('ingesta', 'tablero'):
        r.xgroup_create(STREAM, group, id='0-0', mkstream=True)

def historical():
    start = time.perf_counter()
    n = 0
    with hbase() as conn:
        for batch in batches(source(count=NH)):
            write_batch(conn, batch, protect=False)
            n += len(batch)
            if n % 100_000 == 0:
                print(f'Histórico: {n:,}/{NH:,}', flush=True)
    elapsed = time.perf_counter()-start
    with hbase() as conn:
        counts = {name:count_rows(conn.table(name)) for name in TABLES}
    if any(n != 846906 for n in counts.values()):
        raise AssertionError(f'Histórico incorrecto: {counts}; se esperaban 846906 filas por tabla')
    result = {'eventos': n, 'filas_distintas_por_tabla':846906,
              'segundos': elapsed, 'eventos_s': n/elapsed, 'filas_s_por_tabla':846906/elapsed,
              'filas_medidas':counts, 'puts_logicos': n*2, 'tamano_lote': 1000}
    save('carga', result)
    return result

def produce(events=None, interval=0.025):
    r = redis_client()
    # Preparar la hora antes de iniciar el reloj: saltar el histórico no debe
    # convertirse en una ráfaga inicial para intentar recuperar ese tiempo.
    events = list(source(NH,NV)) if events is None else events
    start = time.perf_counter()
    for n, ev in enumerate(events):
        r.xadd(STREAM, ev, maxlen=MAXLEN, approximate=True)
        r.incr('producidos')
        if interval:
            time.sleep(max(0, start+(n+1)*interval-time.perf_counter()))
    r.set('productor:terminado', '1')
    print(f'Productor: {n+1} eventos, {time.perf_counter()-start:.2f} s', flush=True)

def consume(group, consumer, crash=False, min_idle=5000):
    r = redis_client()
    conn = hbase() if group == 'ingesta' else None
    dashboard = r.register_script(TABLERO_LUA)
    ack = r.register_script(ACK_LUA)
    crash_done = False
    try:
        while True:
            # Recorremos todo el cursor de XAUTOCLAIM; no sólo las primeras 100
            # pendientes, que pueden no tener aún suficiente tiempo de inactividad.
            messages = []
            cursor = '0-0'
            while True:
                claim = r.xautoclaim(STREAM, group, consumer, min_idle, cursor, count=200)
                cursor, recovered = claim[:2]
                if len(claim) > 2 and claim[2]:
                    raise RuntimeError(f'MAXLEN eliminó mensajes pendientes: {claim[2]}')
                messages += recovered
                if cursor == '0-0' or messages:
                    break
            if not messages:
                data = r.xreadgroup(group, consumer, {STREAM: '>'}, count=200, block=300)
                messages = data[0][1] if data else []
            if messages:
                if group == 'ingesta':
                    write_batch(conn, [ev for _, ev in messages])
                    if crash and not crash_done:
                        crash_done = True
                        RUNTIME.mkdir(exist_ok=True)
                        (RUNTIME/'d1_lote.json').write_text(json.dumps(messages), encoding='utf-8')
                        # Falla real, fuera del mecanismo normal de excepciones.
                        os._exit(1)
                    ack(keys=[STREAM], args=[group]+[mid for mid, _ in messages])
                else:
                    for mid, ev in messages:
                        dashboard(keys=[STREAM], args=[ev['estacion_id'],ev['tipo_sensor'],
                                  ev['valor'],ev['ts'],ev['seq'],group,mid])
            info = next(g for g in r.xinfo_groups(STREAM) if g['name'] == group)
            if r.exists('productor:terminado') and info['pending'] == 0 and info['lag'] == 0:
                return
    finally:
        if conn:
            conn.close()

def verify(strict=True):
    r = redis_client()
    result = {'producidos': int(r.get('producidos') or 0), 'stream_xlen': r.xlen(STREAM),
              'grupos': {}, 'filas': {}}
    for group in ('ingesta', 'tablero'):
        info = next(x for x in r.xinfo_groups(STREAM) if x['name'] == group)
        result['grupos'][group] = {'procesadas': int(r.get('procesadas:'+group) or 0),
                                  'xpending': r.xpending(STREAM, group), 'lag': info['lag']}
    with hbase() as conn:
        result['filas'] = {name: count_rows(conn.table(name)) for name in TABLES}
    result['controles_esperados'] = {'producidos': NV, 'filas_por_tabla': 853_953}
    result['ok'] = (result['producidos'] == NV and
                    all(n == 853_953 for n in result['filas'].values()) and
                    all(g['procesadas'] == NV and g['xpending']['pending'] == 0 and g['lag'] == 0
                        for g in result['grupos'].values()))
    result['explicacion'] = ('871200 eventos menos 17243 repetidos exactos y 4 colisiones '
                             'de estación y milisegundo = 853953 claves distintas.')
    save('verificacion', result)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    if strict and not result['ok']:
        raise AssertionError('Los números no calzan: revisar la evidencia, no continuar con el informe.')
    return result

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('accion', choices=['historico','productor','consumidor','verificar'])
    parser.add_argument('--grupo', choices=['ingesta','tablero'], default='ingesta')
    parser.add_argument('--nombre', default='worker-1')
    parser.add_argument('--matar', action='store_true')
    args = parser.parse_args()
    if args.accion == 'historico': historical()
    elif args.accion == 'productor': produce()
    elif args.accion == 'verificar': verify()
    else: consume(args.grupo, args.nombre, args.matar)
