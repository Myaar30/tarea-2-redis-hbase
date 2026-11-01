# HBase 2.5 en modo standalone: HMaster, RegionServer y ZooKeeper en un solo
# proceso, guardando en el disco del contenedor (/data) en vez de HDFS. Más el
# servidor Thrift (puerto 9090), que es por donde habla `happybase` desde Python.
#
# No hay imagen oficial de HBase en Docker Hub: por eso se construye, igual que
# la pmd-postgis-gdal del Lab 05. Una vez, antes de la clase (descarga ~340 MB):
#     docker compose build
FROM eclipse-temurin:11-jre

ARG HBASE_VERSION=2.5.15
RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && f=hbase-${HBASE_VERSION}-bin.tar.gz \
 && (curl -fL --connect-timeout 15 --max-time 600 https://dlcdn.apache.org/hbase/${HBASE_VERSION}/$f -o /tmp/$f \
     || curl -fL --connect-timeout 15 --max-time 600 https://repo.huaweicloud.com/apache/hbase/${HBASE_VERSION}/$f -o /tmp/$f \
     || curl -fL --connect-timeout 15 --max-time 3600 https://archive.apache.org/dist/hbase/${HBASE_VERSION}/$f -o /tmp/$f) \
 && echo '60a07d97ec68203c3494637f4abcb6fdc2fcc75c4c1b88b80d5d8616640e2a57a5193067849f677d441cae558ede69ee4e0ce489816cbcb9117db268fc316ae5  /tmp/hbase-2.5.15-bin.tar.gz' | sha512sum -c - \
 && tar -xzf /tmp/$f -C /opt && mv /opt/hbase-${HBASE_VERSION} /opt/hbase \
 && rm -rf /tmp/$f /opt/hbase/docs

ENV HBASE_HOME=/opt/hbase PATH=/opt/hbase/bin:$PATH

# rootdir en disco local en vez de HDFS; el "unsafe" es porque el disco local no
# garantiza hflush para el WAL (en un cluster real eso lo da HDFS). El discharger
# archiva las HFiles ya compactadas cada 10 s en vez de 2 min: para que el
# Ejercicio 4.3 se vea en clase.
RUN printf '%s\n' '<configuration>' \
  '<property><name>hbase.rootdir</name><value>file:///data/hbase</value></property>' \
  '<property><name>hbase.zookeeper.property.dataDir</name><value>/data/zookeeper</value></property>' \
  '<property><name>hbase.unsafe.stream.capability.enforce</name><value>false</value></property>' \
  '<property><name>hbase.hfile.compaction.discharger.interval</name><value>10000</value></property>' \
  '</configuration>' > /opt/hbase/conf/hbase-site.xml

# 16010: interfaz web del HMaster · 9090: Thrift (happybase)
EXPOSE 16010 9090
WORKDIR /opt/hbase

# Al `docker compose down/restart` (SIGTERM) se apaga ordenado: HBase vacía la
# MemStore a disco antes de salir, y nada se pierde.
CMD ["bash", "-c", "trap 'hbase-daemon.sh stop thrift; stop-hbase.sh; exit 0' TERM; start-hbase.sh && hbase-daemon.sh start thrift && tail -F logs/*.log & wait"]
