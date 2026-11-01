"""Las seis preguntas. Toda respuesta de consulta sale de Redis o HBase."""
from __future__ import annotations
from collections import defaultdict
import json
import time
import urllib.request
from .core import *

def summarize(events):
    acc = defaultdict(list)
    for ev in events:
        acc[ev['estacion_id']].append((int(ev['ts']), float(ev['valor'])))
    result = {}
    for est, rows in sorted(acc.items()):
        maximum = max(v for _, v in rows)
        result[est] = {'n': len(rows), 'promedio': sum(v for _, v in rows)/len(rows),
                       'maximo': maximum, 'hora_maximo': iso(min(t for t,v in rows if v == maximum))}
    return result

def bounds(date, begin, end):
    return ms(f'2026-11-{date}T{begin}:00-03:00'), ms(f'2026-11-{date}T{end}:00-03:00')

def station_range(table, est, start, stop):
    return scan_events(table, row_start=f'{est}#{start:013d}'.encode(),
                       row_stop=f'{est}#{stop:013d}'.encode())

def p1():
    r = redis_client()
    def now_redis():
        with r.pipeline(transaction=False) as pipe:
            for est in sorted(STATIONS): pipe.hgetall('ultimo:'+est)
            pipe.zrevrange('ranking:flujo', 0, 4, withscores=True)
            data = pipe.execute()
        return {'ultimos': dict(zip(sorted(STATIONS), data[:-1])), 'top5': data[-1]}
    with hbase() as conn:
        table = conn.table('lecturas')
        def now_hbase():
            latest = {}
            for est in sorted(STATIONS):
                row = next(table.scan(row_start=f'{est}#9999999999999'.encode(),
                                      row_stop=f'{est}#'.encode(), reverse=True, limit=1))
                ev = decode(row[1])
                latest[est] = {'valor': ev['valor'], 'ts': ev['ts'],
                               'tipo': ev['tipo_sensor'], 'seq': ev['seq']}
            top = sorted([(e,float(v['valor'])) for e,v in latest.items() if v['tipo']==FLOW],
                         key=lambda pair: (pair[1],pair[0]), reverse=True)[:5]
            return {'ultimos': latest, 'top5': top}
        result = {'redis': measure(now_redis), 'hbase': measure(now_hbase)}
    assert result['redis']['resultado']['ultimos'] == result['hbase']['resultado']['ultimos']
    assert result['redis']['resultado']['top5'] == result['hbase']['resultado']['top5']
    result['medicion_vs_llegada'] = {}
    for est, latest in result['redis']['resultado']['ultimos'].items():
        arrival = r.hgetall('llegada:'+est)
        if latest != arrival:
            result['medicion_vs_llegada'][est] = {'medicion': latest, 'llegada': arrival}
    save('p1', result)

def p2():
    r = redis_client()
    stations = sorted(r.smembers('tipo:'+FLOW))
    anchor = int(r.get('tablero:watermark'))
    def query():
        result = {}
        for est in stations:
            rows = r.zrangebyscore('ventana:'+est, anchor-600_000, anchor)
            values = [float(x.split('|')[1]) for x in rows]
            result[est] = {'n': len(values), 'promedio': sum(values)/len(values) if values else None}
        return result
    result = {'consulta': measure(query), 'ancla': iso(anchor),
              'memoria_ventanas_bytes': {e:r.memory_usage('ventana:'+e, samples=0) or 0 for e in stations}}
    # Comparación medida de memoria con una hora retenida. Las consultas anteriores
    # sólo leyeron ventana:*; el generador se usa aquí para construir el control.
    raw = defaultdict(dict)
    for ev in source(NH,NV):
        if ev['tipo_sensor'] == FLOW:
            raw[ev['estacion_id']][ev['ts']] = ev
    memory = {}
    for est, events in raw.items():
        scratch = 'experimento:sin_recorte:'+est
        r.delete(scratch)
        r.zadd(scratch, {f"{ev['ts']}|{ev['valor']}": int(ev['ts']) for ev in events.values()})
        memory[est] = r.memory_usage(scratch, samples=0)
        r.delete(scratch)
    result['memoria_sin_recorte_bytes'] = memory
    result['memoria_auxiliar_secuencias_bytes'] = {e:r.memory_usage('ventana_seq:'+e,samples=0) or 0 for e in stations}
    save('p2', result)

def p3():
    start, stop = bounds('04','08:00','10:00')
    def aggregate(events):
        values = defaultdict(list)
        for ev in events:
            values[(int(ev['ts'])-start)//900_000].append(float(ev['valor']))
        return {'n': sum(map(len,values.values())), 'cuartos': [
            {'desde': iso(start+i*900_000), 'n': len(values[i]),
             'promedio': sum(values[i])/len(values[i]) if values[i] else None} for i in range(8)]}
    with hbase() as conn:
        table = conn.table('lecturas')
        ranged = measure(lambda: aggregate(station_range(table,'CC-001',start,stop)))
        def full():
            return aggregate(ev for ev in scan_events(table)
                             if ev['estacion_id']=='CC-001' and start<=int(ev['ts'])<stop)
        print('P3: midiendo seis recorridos completos de HBase', flush=True)
        fullscan = measure(full)
    assert ranged['resultado'] == fullscan['resultado']
    save('p3', {'rango': ranged, 'tabla_entera': fullscan,
                'factor': fullscan['mediana_ms']/ranged['mediana_ms']})

def disk_space():
    shell("flush 'lecturas'\nflush 'lecturas_tipo'")
    # Directorios de tablas tras flush, incluidos metadatos de región;
    # no confundir esta medida con el WAL ni con la MemStore.
    result = {}
    for name in TABLES:
        out = compose('exec','-T','hbase','du','-sb',f'/data/hbase/data/default/{name}').stdout
        result[name] = int(out.split()[0])
    return result

def p4():
    r = redis_client()
    start, stop = bounds('05','07:00','09:00')
    with hbase() as conn:
        def by_station():
            # Incluir el acceso al catálogo dentro del tiempo de a).
            stations = sorted(r.smembers('tipo:'+AIR))
            return summarize(ev for est in stations for ev in station_range(conn.table('lecturas'),est,start,stop))
        def by_type():
            return summarize(scan_events(conn.table('lecturas_tipo'),
                             row_start=f'{AIR}#{start:013d}#'.encode(),
                             row_stop=f'{AIR}#{stop:013d}#'.encode()))
        a, b = measure(by_station), measure(by_type)
    assert a['resultado'] == b['resultado']
    save('p4', {'estacion':a,'tipo':b,'espacio_bytes':disk_space(),
               'escrituras_extra_solicitadas': NH+NV})

def jmx_regions(conn, name):
    url = 'http://127.0.0.1:16010/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions'
    with urllib.request.urlopen(url, timeout=30) as response:
        beans = json.load(response)['beans']
    metrics = {k:v for bean in beans for k,v in bean.items()}
    result = {}
    for region in conn.table(name).regions():
        encoded = region['name'].decode().split('.')[-2]
        metric = f'Namespace_default_table_{name}_region_{encoded}_metric_writeRequestCount'
        if metric not in metrics:
            raise RuntimeError(f'JMX todavía no publica {metric}')
        result[region['start_key'].decode() or '(inicio)'] = metrics[metric]
    return result

def p5():
    create_tables(P5_TABLES, reset=True)
    live = list(source(NH,NV))
    with hbase() as conn:
        # Métrica inicial de tablas nuevas es cero. Esperar publicación real.
        time.sleep(12)
        before = {name:jmx_regions(conn,name) for name in P5_TABLES}
        write_times = {}
        for name in P5_TABLES:
            start = time.perf_counter()
            for batch in batches(live,200): write_batch(conn,batch,[name],protect=False)
            write_times[name] = time.perf_counter()-start
        time.sleep(12)
        after = {name:jmx_regions(conn,name) for name in P5_TABLES}
        delta = {name:{k:v-before[name][k] for k,v in after[name].items()} for name in P5_TABLES}
        start, stop = bounds('07','00:10','00:20')
        def all_station():
            return sorted((ev['estacion_id'],ev['ts'],ev['valor']) for est in sorted(STATIONS)
                           for ev in station_range(conn.table('p5_estacion'),est,start,stop))
        def time_first():
            return sorted((ev['estacion_id'],ev['ts'],ev['valor']) for ev in scan_events(conn.table('p5_ts'),
                          row_start=f'{start:013d}#'.encode(), row_stop=f'{stop:013d}#'.encode()))
        def salted():
            return sorted((ev['estacion_id'],ev['ts'],ev['valor']) for s in range(4)
                           for ev in scan_events(conn.table('p5_salt'),row_start=f'{s}#{start:013d}#'.encode(),
                                                 row_stop=f'{s}#{stop:013d}#'.encode()))
        queries = {name:measure(fn) for name,fn in zip(P5_TABLES,(all_station,time_first,salted))}
    assert queries['p5_estacion']['resultado'] == queries['p5_ts']['resultado'] == queries['p5_salt']['resultado']
    for v in queries.values():
        # Resultados completos de lecturas se regeneran: guardamos un resumen verificable.
        rows = v.pop('resultado')
        v['resultado'] = {'n':len(rows),'sha256':hashlib.sha256(json.dumps(rows).encode()).hexdigest()}
    save('p5', {'escrituras_region_jmx':delta,'carga_segundos':write_times,'consultas':queries,
                'salt_estaciones': {e:salt(e) for e in sorted(STATIONS)},
                'nota':'JMX cuenta mutaciones realmente enviadas: duplicados dentro de un lote se consolidan.'})

def p6():
    # Pregunta propia: ¿qué comunas tienen ahora mayor dispersión de flujo entre
    # estaciones y, para la comuna ganadora, cuándo ocurrió su pico agregado ayer?
    # Redis decide qué comuna; HBase decide cuándo con datos históricos simultáneos.
    r = redis_client()
    start, stop = bounds('06','00:00','01:00')
    def query():
        communes = defaultdict(list)
        for est in sorted(r.smembers('tipo:'+FLOW)):
            row = r.hgetall('ultimo:'+est)
            communes[est.split('-')[0]].append((est,float(row['valor'])))
        candidates = {c:max(v for _,v in rows)-min(v for _,v in rows)
                      for c,rows in communes.items() if len(rows)>=2}
        winner = sorted(candidates, key=lambda c:(-candidates[c],c))[0]
        stations = [e for e,_ in communes[winner]]
        buckets = defaultdict(lambda:defaultdict(list))
        with hbase() as conn:
            for est in stations:
                for ev in station_range(conn.table('lecturas'),est,start,stop):
                    buckets[(int(ev['ts'])-start)//300_000][est].append(float(ev['valor']))
        aggregate = {i:sum(sum(v)/len(v) for v in by_station.values())
                     for i,by_station in buckets.items() if set(by_station)==set(stations)}
        peak = sorted(aggregate, key=lambda i:(-aggregate[i],i))[0]
        return {'dispersion_por_comuna': candidates,'comuna':winner,'estaciones':stations,
                'desde':iso(start+peak*300_000),'hasta':iso(start+(peak+1)*300_000),
                'suma_promedios_flujo':aggregate[peak]}
    save('p6', measure(query))

def run_queries():
    for i,fn in enumerate((p1,p2,p3,p4,p5,p6),1):
        print(f'Resolviendo P{i}',flush=True)
        fn()

if __name__ == '__main__': run_queries()
