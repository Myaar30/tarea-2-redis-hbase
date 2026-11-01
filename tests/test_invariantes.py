import unittest
from tarea.core import key, normalize, salt, ms, FLOW
from tarea.pipeline import TABLERO_LUA

class ClavesTest(unittest.TestCase):
    def test_clave_no_depende_de_llegada(self):
        ev={'estacion_id':'CC-001','tipo_sensor':FLOW,'valor':'5','ts':'1793590200000','seq':'1'}
        later={**ev,'seq':'99'}
        for table in ('lecturas','lecturas_tipo','p5_estacion','p5_ts','p5_salt'):
            self.assertEqual(key(ev,table),key(later,table))

    def test_semiabierto_y_offset(self):
        self.assertEqual(ms('2026-11-04T08:00:00-03:00'),ms('2026-11-04T11:00:00+00:00'))
        self.assertEqual(ms('2026-11-04T10:00:00-03:00')-ms('2026-11-04T08:00:00-03:00'),7200000)

    def test_salt_acotado_y_estable(self):
        import os,subprocess,sys
        self.assertIn(salt('CC-001'),range(4))
        outputs=[]
        for seed in ('1','2'):
            outputs.append(subprocess.check_output([sys.executable,'-c',
                "from tarea.core import salt; print(salt('CC-001'))"],
                env={**os.environ,'PYTHONHASHSEED':seed},text=True))
        self.assertEqual(outputs[0],outputs[1])

class TableroRealTest(unittest.TestCase):
    def test_tardia_duplicada_colision_y_recorte_global(self):
        import redis
        # DB 15 es exclusivamente de prueba; el pipeline usa DB 0.
        r=redis.Redis(host='127.0.0.1',port=6379,db=15,decode_responses=True)
        r.flushdb()
        stream='prueba'
        r.xgroup_create(stream,'tablero',id='0',mkstream=True)
        fn=r.register_script(TABLERO_LUA)
        def send(est,ts,seq,val):
            mid=r.xadd(stream,{'x':'1'})
            r.xreadgroup('tablero','test',{stream:'>'})
            fn(keys=[stream],args=[est,FLOW,str(val),str(ts),str(seq),'tablero',mid])
        try:
            send('A',1000000,1,10)
            send('A',999000,2,99)
            self.assertEqual(r.hget('ultimo:A','valor'),'10')
            send('A',1000000,1,10)
            self.assertEqual(r.zcard('ventana:A'),2)
            send('A',1000000,3,12)
            send('A',1000000,1,10)
            self.assertEqual(r.hget('ultimo:A','valor'),'12')
            self.assertEqual(r.zcard('ventana:A'),2)
            send('B',1600001,4,7)
            self.assertEqual(r.zcard('ventana:A'),0)
            self.assertEqual(r.hlen('ventana_seq:A'),0)
            self.assertEqual(r.xpending(stream,'tablero')['pending'],0)
        finally:
            r.flushdb()

if __name__=='__main__': unittest.main()
