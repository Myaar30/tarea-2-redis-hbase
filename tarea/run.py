"""Un comando ejecuta carga, streaming, consultas, fallas e informe."""
from __future__ import annotations
import argparse
import hashlib
import json
import platform
import time
from .core import *

def audit_source():
    exact, keys = set(), set()
    result = {}
    start = time.perf_counter()
    live = []
    for label,count,offset in [('historico',NH,0),('vivo',NV,NH)]:
        duplicates = 0
        phasekeys = set()
        phase_exact = set()
        # Un único iterador conserva exactamente el estado del RNG entre fases.
        for ev in source(offset,count):
            identity = f"{ev['estacion_id']}|{ev['tipo_sensor']}|{ev['ts']}|{ev['valor']}"
            signature = hashlib.sha256(identity.encode()).digest()
            duplicates += signature in phase_exact
            phase_exact.add(signature)
            exact.add(signature)
            k = key(ev)
            phasekeys.add(k)
            keys.add(k)
            if label=='vivo': live.append(ev)
        result[label] = {'eventos':count,'repetidos':duplicates,'claves_distintas':len(phasekeys)}
    result['total'] = {'eventos':NH+NV,'repetidos':NH+NV-len(exact),'claves_distintas':len(keys)}
    result['segundos'] = time.perf_counter()-start
    assert result['historico']=={'eventos':NH,'repetidos':17090,'claves_distintas':846906}
    assert result['vivo']=={'eventos':NV,'repetidos':153,'claves_distintas':7047}
    assert result['total']=={'eventos':NH+NV,'repetidos':17243,'claves_distintas':853953}
    save('generacion',result)
    RUNTIME.mkdir(exist_ok=True)
    (RUNTIME/'vivo.json').write_text(json.dumps(live),encoding='utf-8')
    return result

def hardware():
    import psutil
    docker = json.loads(subprocess.check_output(['docker','info','--format','{{json .}}'],text=True))
    cpu = platform.processor()
    if os.name=='nt':
        import winreg
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,r'HARDWARE\DESCRIPTION\System\CentralProcessor\0') as reg:
            cpu = winreg.QueryValueEx(reg,'ProcessorNameString')[0].strip()
    return {'sistema':platform.platform(),'cpu':cpu,'ram_gib':round(psutil.virtual_memory().total/2**30,2),
            'python':platform.python_version(),'cpu_docker':docker['NCPU'],
            'ram_docker_gib':round(docker['MemTotal']/2**30,2),'docker':docker['ServerVersion'],
            'hbase':'2.5.15 standalone','redis':'7.4.2','fecha':datetime.now(TZ).isoformat()}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reutilizar-historico',action='store_true',
                        help='Desarrollo: verifica 846906 filas antes de reutilizar; no usar después del vivo')
    parser.add_argument('--solo-consultas',action='store_true')
    args = parser.parse_args()
    start = time.perf_counter()
    from .pipeline import historical,verify
    from .experimentos import live_and_d1,d2
    from .consultas import run_queries
    save('equipo',hardware())
    if not args.solo_consultas:
        audit_source()
        if args.reutilizar_historico:
            with hbase() as conn:
                assert all(count_rows(conn.table(t))==846906 for t in TABLES)
        else:
            create_tables(TABLES,reset=True)
            historical()
        live_and_d1()
    run_queries()
    d2()
    verify()
    save('ejecucion',{'segundos_totales':time.perf_counter()-start,'completa':True})
    from .informe import build_report
    build_report()
    print('Entrega generada: informe.pdf y resultados verificables.',flush=True)

if __name__ == '__main__': main()
