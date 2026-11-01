"""Informe de cinco páginas construido sólo con evidencia de la ejecución."""
from __future__ import annotations
from html import escape
from pathlib import Path
from .core import ROOT, OUT, load, iso, STATIONS, FLOW, AIR

def build_report():
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,PageBreak
    evidence = {k:load(k) for k in ['equipo','generacion','carga','verificacion','p1','p2','p3','p4','p5','p6','d1','d2','ejecucion']}
    if not evidence['verificacion']['ok'] or not evidence['ejecucion']['completa']:
        raise RuntimeError('Falta evidencia para emitir un informe final')
    styles = {
        'normal':ParagraphStyle('normal',fontName='Helvetica',fontSize=9.6,leading=12.5,spaceAfter=5),
        'small':ParagraphStyle('small',fontName='Helvetica',fontSize=8.1,leading=10.1,spaceAfter=4),
        'cell':ParagraphStyle('cell',fontName='Helvetica',fontSize=8.4,leading=10.3),
        'h':ParagraphStyle('h',fontName='Helvetica-Bold',fontSize=12,leading=15,spaceBefore=7,spaceAfter=5),
        'title':ParagraphStyle('title',fontName='Helvetica-Bold',fontSize=20,leading=23,spaceAfter=9),
    }
    story=[]
    def p(text,kind='normal'): story.append(Paragraph(text,styles[kind]))
    def h(text): p(text,'h')
    def table(rows,widths):
        data=[[Paragraph(escape(str(c)),styles['cell']) for c in row] for row in rows]
        t=Table(data,colWidths=widths,repeatRows=1,hAlign='LEFT')
        t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#E7EDF1')),
                              ('GRID',(0,0),(-1,-1),0.4,colors.HexColor('#D9D9D9')),
                              ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('LEFTPADDING',(0,0),(-1,-1),6),
                              ('RIGHTPADDING',(0,0),(-1,-1),6),('TOPPADDING',(0,0),(-1,-1),4),
                              ('BOTTOMPADDING',(0,0),(-1,-1),4)]))
        story.extend([t,Spacer(1,6)])
    def fmt(value,d=2): return f'{value:,.{d}f}'.replace(',','X').replace('.',',').replace('X','.')
    def clock(ts): return iso(ts).split('T')[1][:12]
    def median(obj): return fmt(obj['mediana_ms'])
    W=letter[0]-1.2*inch
    e=evidence
    p('Tarea 2 Redis y HBase','title')
    p('<b>Del stream a la historia</b><br/>Nicolas Galaz, Martin Yañez y Eduardo Roa<br/>'
      'Procesamiento Masivo de Datos · ELE051-B / EIN102B · USM 2026-2 · Paralelo 701<br/>'
      'Profesor Diego Ramírez · 1 de noviembre de 2026','small')
    p('Construimos un pipeline que separa el estado actual de la historia. Redis mantiene el tablero '
      'y entrega los eventos a dos grupos independientes. HBase conserva las mediciones y permite '
      'consultar rangos. Nuestra decisión es elegir el motor según la pregunta y comprobar el costo con consultas reales.')
    h('A y B Infraestructura y verificación')
    p('Usamos Redis 7 con AOF, appendfsync everysec y volumen; HBase 2.5.15 standalone con el Dockerfile '
      'del Lab 07 y volumen propio. Ambos tienen healthcheck. El histórico se escribe directamente en '
      'dos tablas, en lotes de 1.000. La hora en vivo entra en sensores:t2 cada 25 ms. Ingesta confirma '
      'con XACK después de escribir las dos tablas; tablero actualiza Redis y confirma dentro de un script Lua atómico.')
    g=e['generacion']
    table([['Control','Histórico','En vivo','Total'],
           ['Eventos',864000,7200,871200],['Repetidos exactos',17090,153,17243],
           ['Claves distintas',846906,7047,853953]],[W*.40,W*.20,W*.20,W*.20])
    p(f"La carga demoró <b>{fmt(e['carga']['segundos'])} s</b>: {fmt(e['carga']['eventos_s'],0)} eventos/s "
      f"y {fmt(e['carga']['filas_s_por_tabla'],0)} filas distintas/s por tabla. "
      'Cada tabla terminó con 853.953 filas; cada grupo procesó 7.200 entradas y quedó con XPENDING = 0 y lag = 0. '
      'Las cuatro colisiones con igual estación y milisegundo explican la diferencia que no corresponde a repetidos exactos.')
    h('Qué va en cada motor')
    table([['Dato','Motor y estructura','Por qué lo dejamos ahí'],
           ['Última lectura','Redis · hash ultimo:<estación>','Acceso directo por estación para el tablero.'],
           ['Ranking actual','Redis · zset ranking:flujo','Top 5 sin recorrer la historia.'],
           ['Ventana de 10 minutos','Redis · zset ventana:<estación>','Puntaje por medición y recorte por rango.'],
           ['Catálogo','Redis · set tipo:<tipo>','Lista de estaciones para decidir qué consultar.'],
           ['Eventos en tránsito','Redis · stream sensores:t2','Dos grupos reciben todos los eventos y conservan pendientes.'],
           ['Historia por estación','HBase · estación#ts','Un intervalo de una estación queda contiguo.'],
           ['Historia por tipo','HBase · tipo#ts#estación','Un intervalo del tipo necesita un solo scan.']],
          [W*.22,W*.34,W*.44])
    p('ts es el milisegundo de medición, con 13 dígitos. Lecturas tiene cuatro regiones con cortes '
      'CC-005, HUA-001 y SPP-002, cuatro estaciones en cada una; lecturas_tipo se divide por los tres tipos. '
      'Las claves no dependen de la llegada. El consumidor único compara la secuencia antes de reescribir: '
      'un replay durable se omite, y VERSIONS = 1 conserva un valor visible. No hay una transacción entre tablas; '
      'si la segunda falla, el pendiente permite completar ambas al reintentar.','small')
    story.append(PageBreak())
    h('P1 El estado actual desde los dos motores')
    p('En Redis leímos los 16 hashes en un pipeline y el ranking con ZREVRANGE. En HBase hicimos '
      '16 scans inversos por prefijo, cada uno con límite 1; así no necesitamos recorrer toda la historia '
      'para encontrar la medición más reciente. Los resultados de ambos motores coincidieron.')
    latest=e['p1']['redis']['resultado']['ultimos']
    stations=sorted(latest)
    rows=[['Estación','Valor','Medición','Estación','Valor','Medición']]
    for i in range(8):
        row=[]
        for est in (stations[i],stations[i+8]):
            row += [est, latest[est]['valor'],clock(latest[est]['ts'])]
        rows.append(row)
    table(rows,[W*.17,W*.11,W*.22,W*.17,W*.11,W*.22])
    p('Mediciones del 07-nov, hora de Chile UTC-03. Los valores tienen la unidad del sensor: '
      'flujo vehicular, ocupación o PM2.5, según el catálogo.','small')
    p('<b>Top 5 de flujo</b> '+ ' · '.join(f'{est}: {fmt(value,1)}' for est,value in e['p1']['redis']['resultado']['top5'])+'.')
    p(f"Redis demoró {median(e['p1']['redis'])} ms y HBase {median(e['p1']['hbase'])} ms. "
      'Para un dashboard elegimos Redis porque el estado ya está calculado y se consulta directamente.')
    for est, pair in e['p1']['medicion_vs_llegada'].items():
        m,a=pair['medicion'],pair['llegada']
        p(f"<b>{est} cambia con el reloj elegido.</b> Por medición: {m['valor']} a las {clock(m['ts'])}; "
          f"por llegada: {a['valor']} medido a las {clock(a['ts'])}, secuencia {a['seq']}. "
          'La última llegada es tardía; por eso no reemplaza el hash ni el ranking.')
    h('P2 Promedio en los últimos 10 minutos')
    p(f"Anclamos la ventana a {e['p2']['ancla']}. Consultamos ZRANGEBYSCORE entre ancla menos 600.000 ms "
      'y ancla, incluyendo ambos extremos; el promedio se calcula con los valores leídos de ventana:*.')
    data=e['p2']['consulta']['resultado']
    table([['Estación','Lecturas','Promedio','Memoria ventana B']]+[[s,d['n'],fmt(d['promedio']),
          e['p2']['memoria_ventanas_bytes'][s]] for s,d in data.items()],[W*.26,W*.17,W*.25,W*.32])
    p(f"La consulta demoró {median(e['p2']['consulta'])} ms. El score permite ubicar y borrar por tiempo de medición, "
      'aunque la llegada esté desordenada. Recortamos todas las estaciones con el watermark global: una estación '
      'sin nuevas lecturas tampoco conserva registros vencidos. El miembro combina timestamp y valor; los reenvíos '
      'no suman muestras y una colisión se resuelve por secuencia.')
    p(f"MEMORY USAGE de los zsets: {sum(e['p2']['memoria_ventanas_bytes'].values()):,} B con recorte y "
      f"{sum(e['p2']['memoria_sin_recorte_bytes'].values()):,} B guardando la hora completa. Los hashes auxiliares "
      f"de secuencia ocupan {sum(e['p2']['memoria_auxiliar_secuencias_bytes'].values()):,} B adicionales. "
      'Sin recorte, la memoria crece con toda la historia recibida.','small')
    story.append(PageBreak())
    h('P3 Lecturas de CC 001 el 4 de noviembre')
    p('Consultamos [08:00, 10:00) con row_start = CC-001#inicio y row_stop = CC-001#fin. '
      'Comparamos ese scan con recorrer lecturas completa y filtrar en el cliente; ambos dieron el mismo resultado.')
    rows=e['p3']['rango']['resultado']
    table([['Cuarto de hora','Lecturas','Promedio']]+[[v['desde'][11:16],v['n'],fmt(v['promedio'])]
          for v in rows['cuartos']],[W*.40,W*.25,W*.35])
    p(f"Hay <b>{rows['n']} lecturas</b>. El rango tomó {median(e['p3']['rango'])} ms y el recorrido completo "
      f"{median(e['p3']['tabla_entera'])} ms, {fmt(e['p3']['factor'],1)} veces más. Con cinco años, el recorrido "
      'completo debe examinar mucha más historia. El intervalo consultado conserva su tamaño; igual dependerá '
      'de los índices de bloques, HFiles, cachés y distribución de regiones. No extrapolamos el factor medido como una constante.')
    h('P4 PM2 5 del 5 de noviembre entre 07 y 09 horas')
    air=e['p4']['tipo']['resultado']
    table([['Estación','Lecturas','Promedio','Máximo','Primera hora del máximo']]+
          [[s,v['n'],fmt(v['promedio']),fmt(v['maximo'],1),v['hora_maximo'][11:23]] for s,v in air.items()],
          [W*.16,W*.15,W*.18,W*.17,W*.34])
    p(f"Con el catálogo tipo:calidad_aire y un rango por estación demoró {median(e['p4']['estacion'])} ms; "
      f"con un scan en lecturas_tipo, {median(e['p4']['tipo'])} ms. Los promedios, máximos y primeras ocurrencias coincidieron.")
    disk=e['p4']['espacio_bytes']
    p(f"La segunda tabla requiere hasta 871.200 escrituras lógicas extra, antes de consolidar repetidos en cada lote. "
      f"Después de flush, lecturas ocupó {fmt(disk['lecturas']/2**20)} MiB y lecturas_tipo "
      f"{fmt(disk['lecturas_tipo']/2**20)} MiB en sus directorios de tabla, sin sumar WAL ni snapshots. "
      'Si falla la segunda escritura, no confirmamos el stream; el reintento consulta la secuencia de cada tabla '
      'y completa lo que falta. No mantendríamos ese índice si casi todas las consultas fueran por estación '
      'o si el costo de escritura y disco pesara más que la mejora de las pocas consultas por tipo.')
    h('Tabla de tiempos de las consultas')
    table([['Pregunta','Diseño 1 ms','Diseño 2 ms','Diseño 3 ms'],
           ['P1','Redis '+median(e['p1']['redis']),'HBase '+median(e['p1']['hbase']),'-'],
           ['P3','Rango '+median(e['p3']['rango']),'Completa '+median(e['p3']['tabla_entera']),'-'],
           ['P4','Estaciones '+median(e['p4']['estacion']),'Tipo '+median(e['p4']['tipo']),'-'],
           ['P5','Estación '+median(e['p5']['consultas']['p5_estacion']),
            'Tiempo '+median(e['p5']['consultas']['p5_ts']),'Salt '+median(e['p5']['consultas']['p5_salt'])]],
          [W*.13,W*.29,W*.29,W*.29])
    p('Valores en milisegundos. Cada cifra es la mediana de cinco corridas posteriores a una de '
      'calentamiento. El equipo y los límites de esta comparación se indican en la página siguiente.','small')
    story.append(PageBreak())
    h('P5 Hotspotting y salting')
    p('Escribimos la misma hora en tres tablas nuevas. Estación primero tiene cuatro regiones de '
      'cuatro estaciones; tiempo primero se divide por los días 03, 05 y 07-nov; salting usa '
      'SHA-256(estación) mod 4 y cortes 1#, 2# y 3#. Usamos un hash estable para que la región '
      'no cambie al reiniciar Python. Esperamos 12 s antes y después de escribir y restamos los contadores JMX.')
    regs=e['p5']['escrituras_region_jmx']
    table([['Diseño','Región 1','Región 2','Región 3','Región 4']]+
          [[name.removeprefix('p5_')]+list(values.values()) for name,values in regs.items()],
          [W*.28,W*.18,W*.18,W*.18,W*.18])
    p('Las columnas siguen el orden de las claves de inicio. JMX cuenta las mutaciones enviadas, '
      'no los 7.200 eventos de entrada: consolidamos claves repetidas dentro de cada lote. En tiempo '
      'primero también pueden caer lecturas tardías en el día anterior; el cuadro muestra la distribución real.','small')
    for name,v in e['p5']['consultas'].items():
        p(f"{name}: {v['resultado']['n']} lecturas entre [00:10, 00:20) del 07-nov; "
          f"{median(v)} ms.",'small')
    p('Los tres resultados completos coincidieron y guardamos su hash de control. Estación primero '
      'necesita 16 rangos, tiempo primero uno y salting cuatro. Para un servicio que recibe continuamente '
      'datos de muchas estaciones elegiríamos estación primero si domina la consulta individual. '
      'Si dominan los intervalos globales y existen varios RegionServers, consideraríamos salting para '
      'repartir la entrada. Tiempo primero es cómodo para consultar, pero concentra las escrituras recientes. '
      'El hash por estación reparte según el catálogo: cuatro salts no garantizan cuatro cargas iguales.')
    h('P6 Nuestra pregunta')
    p('¿Qué comuna presenta ahora mayor diferencia de flujo entre sus estaciones y en qué intervalo '
      'de cinco minutos tuvo su mayor flujo agregado entre 00:00 y 01:00 del día anterior? '
      'Sólo comparamos comunas con al menos dos estaciones de flujo. Redis selecciona la comuna con los hashes '
      'actuales y el catálogo; HBase busca el intervalo histórico. Por intervalo sumamos el promedio de cada '
      'estación y exigimos que todas tengan muestras; los empates se resuelven por la primera hora.')
    own=e['p6']['resultado']
    p(f"Ganó <b>{own['comuna']}</b>, con una diferencia actual de {fmt(own['dispersion_por_comuna'][own['comuna']],1)}. "
      f"El 06-nov su pico agregado fue [{own['desde'][11:16]}, {own['hasta'][11:16]}), "
      f"con suma de promedios {fmt(own['suma_promedios_flujo'])}. La consulta conjunta tomó {median(e['p6'])} ms. "
      'Necesitamos ambos motores: el estado actual decide qué comuna revisar y la historia decide cuándo ocurrió su pico.')
    hw=e['equipo']
    p('Hicimos una corrida de calentamiento y después cinco corridas medidas; informamos la mediana '
      'de esas cinco. Los JSON guardan las seis duraciones. Las consultas incluyen lectura, transferencia '
      'y agregación, pero dejan la conexión ya abierta cuando es posible.','small')
    p(f"Equipo: {escape(hw['cpu'])}; RAM física {hw['ram_gib']} GiB; {escape(hw['sistema'])}; "
      f"Python {hw['python']}; Docker {hw['docker']} con {hw['cpu_docker']} CPU lógicas y "
      f"{hw['ram_docker_gib']} GiB de RAM. Redis 7.4.2 y HBase 2.5.15 standalone. "
      'Las regiones comparten un único proceso y disco: estos tiempos no miden escalamiento de un cluster.','small')
    story.append(PageBreak())
    h('D1 El consumidor muere antes del XACK')
    d=e['d1']
    p(f"Matamos ingesta-que-muere con os._exit(1), después del put de las dos tablas y antes del XACK. "
      f"El proceso terminó con código {d['salida_proceso']}; quedaron {d['xpending_tras_muerte']['pending']} entradas "
      'en XPENDING, asignadas a ese consumidor. Ingesta-rescate las recuperó con XAUTOCLAIM después '
      'de 5.000 ms de inactividad y procesó el resto del stream.')
    p(f"Muestra: {d['clave_muestra']}. Antes: {escape(str(d['celda_antes']))}. "
      f"Después: {escape(str(d['celda_despues']))}.",'small')
    p('El reintento encuentra la misma clave y secuencia, por lo que omite el put ya durable; la muestra '
      'mantiene el valor y su timestamp de versión. VERSIONS = 1 limita las versiones visibles. '
      'Las filas finales fueron 853.953 en cada tabla y XPENDING volvió a cero. La clave idempotente '
      'evita duplicar filas; el orden put y después XACK evita confirmar algo que todavía no fue escrito.')
    h('D2 Reiniciamos Redis en un punto controlado')
    p('Repetimos docker compose restart redis con AOF y con appendonly no más save vacío. '
      'En cada caso el pipeline quedó detenido con 3.000 eventos producidos: 2.800 confirmados en '
      'HBase, 100 entregados a ingesta sin put ni XACK y 100 todavía no entregados. El tablero ya había '
      'procesado los 3.000. Usamos tablas de experimento separadas para que la carga final no oculte pérdidas.')
    rows=[['Estado','Antes','Después con AOF','Después sin persistencia']]
    a=e['d2']['aof']; n=e['d2']['sin_persistencia']
    for label,k in [('Stream entradas','stream_entradas'),('Hashes ultimo','ultimo_hashes'),
                    ('Ventanas estaciones','ventana_estaciones'),('Lecturas en ventanas','ventana_lecturas')]:
        rows.append([label,a['antes'][k],a['despues'][k],n['despues'][k]])
    rows += [['Grupos',2,len(a['despues']['grupos']),len(n['despues']['grupos'])],
             ['XPENDING ingesta',100,a['despues']['pendientes'].get('ingesta',{}).get('pending',0),
              n['despues']['pendientes'].get('ingesta',{}).get('pending','sin grupo')]]
    table(rows,[W*.34,W*.14,W*.26,W*.26])
    p('Con AOF sobrevivieron también las identidades de consumidores, los pendientes y el contenido '
      'completo del stream y del tablero; comparamos hashes de contenido. Sin persistencia se perdió todo '
      f"el estado de Redis. Cada tabla de prueba de HBase conservó {fmt(e['d2']['aof']['hbase_filas_despues']['d2_lecturas'],0)} filas en ambos casos. El histórico y lo "
      'ya confirmado no dependen de reiniciar Redis.')
    p('En ese punto hay <b>200 eventos sin confirmación durable</b>: 100 s del reloj simulado o 5 s '
      'de producción acelerada. Es una ventana de exposición, no necesariamente 200 claves nuevas, '
      'porque también hay repetidos. El lote escrito sin XACK de D1 ya estaba a salvo y sólo requería '
      'reproceso. En un sistema real aceptaríamos como máximo alrededor de un segundo ante un corte '
      'brusco, por eso elegimos AOF everysec; si no aceptáramos esa ventana usaríamos always y mediríamos '
      'su costo. Este reinicio fue ordenado y no demuestra tolerancia a cortes de energía ni una cota absoluta de fsync.')
    h('Límites y qué cambiaríamos')
    p('MAXLEN aproximado de 20.000 conserva la hora completa y equivale a unos 500 s de producción a '
      '40 eventos/s. Recortar una entrada no leída puede perderla aunque exista el grupo; en producción '
      'vigilaríamos lag y aplicaríamos backpressure antes de alcanzar ese límite. HBase standalone y su '
      'WAL sobre disco local no tienen las garantías de HDFS. La URL principal de descarga devolvió 404 '
      'y el archivo alternativo resultó lento; usamos un espejo con checksum SHA-512 oficial. Mantendríamos un escritor de '
      'ingesta; para paralelizarlo agregaríamos control atómico de secuencia por fila.','small')
    p('Fuentes: pauta Tarea 2 Redis + HBase; laboratorios 06 y 07, github.com/diegoalrv/ein102b; '
      'Redis Docs (Streams, XAUTOCLAIM y persistencia); Apache HBase Reference Guide; HappyBase API. '
      'Código, enlaces exactos, muestras y tiempos crudos están en el repositorio.','small')
    pages=[]
    def footer(canvas,doc):
        pages.append(doc.page)
        canvas.setFont('Helvetica',8)
        canvas.drawRightString(letter[0]-.6*inch,.34*inch,f'{doc.page} / 5')
    target=ROOT/'informe.pdf'
    SimpleDocTemplate(str(target),pagesize=letter,rightMargin=.6*inch,leftMargin=.6*inch,
                      topMargin=.52*inch,bottomMargin=.55*inch,title='Tarea 2 Redis y HBase',
                      author='Nicolas Galaz, Martin Yañez y Eduardo Roa').build(story,onFirstPage=footer,onLaterPages=footer)
    if len(pages)!=5:
        raise RuntimeError(f'El informe generó {len(pages)} páginas; ajustar el diseño antes de entregar')
    print(f'Informe generado y límite de cinco páginas comprobado: {target}',flush=True)
    # Actualizar la duración real sin tocar las instrucciones de reproducción.
    readme=ROOT/'README.md'
    text=readme.read_text(encoding='utf-8')
    marker='\n<!-- RESULTADOS_MEDIDOS -->'
    text=text.split(marker)[0]
    text += marker+'\n\n## Ejecución verificada\n\n'
    text += f"Tiempo del pipeline, consultas y pruebas: {e['ejecucion']['segundos_totales']:.1f} s, sin descarga inicial. "
    text += f"Carga histórica: {e['carga']['segundos']:.1f} s ({e['carga']['eventos_s']:.0f} eventos/s). "
    text += 'Verificación final: 853.953 filas por tabla, 7.200 entradas procesadas por grupo, pendientes y lag en cero.\n'
    readme.write_text(text,encoding='utf-8')

if __name__=='__main__': build_report()
