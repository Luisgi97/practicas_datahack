"""APARTADO 1.1"""

"""Listamos las databases que se encuentran en cdm1/web"""
sqoop list-databases --connect jdbc:mysql://cdm1/web \
--username datahack \
--P

"""Listamos las tablas en cdm1/web"""
sqoop list-tables --connect jdbc:mysql://cdm1/web \
--username datahack \
--P

"""Importamos la tabla compras desde cdm1/web a HDFS"""
sqoop import --connect jdbc:mysql://cdm1/web \
--username datahack \
--P \
--table compras \
--target-dir hdfs:///user/luis.blanco/import_sql \
-m 1

"""Vemos el nombre de la tabla"""
hdfs dfs -ls import_sql/

"""Vemos la tabla para ver los campos"""
hdfs dfs -cat import_sql/part-m-00000 | head -50

"""Entramos en Hive y usamos el esquema luis_blanco (aprovechamos el creado durante una práctica de clase)"""
hive
use luis_blanco;

"""Creamos la tabla externa bbdd en Hive, cuyos datos se encuentran en hdfs:///user/luis.blanco/import_sql"""
CREATE EXTERNAL TABLE bbdd(
fecha TIMESTAMP,
tipo_pago STRING)
COMMENT 'Esta es la tabla bbdd'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/luis.blanco/import_sql';

"""Lanzamos una query para ver que tengamos registros en la tabla, observamos que hay 5837"
SELECT COUNT (*) from bbdd;

"""Lanzamos una query para ver los distintos tipos de pago que tenemos, observamos que hay 6"""
SELECT COUNT (DISTINCT tipo_pago) from bbdd;

"""Hacemos un group by para ver qué tipos de pago tenemos en la tabla:"""
SELECT tipo_pago, COUNT (tipo_pago) from bbdd GROUP BY tipo_pago;

"""y tenemos el siguiente resultado:
        185 VACIO
comprar=COMPRAR 800 ERROR
contrareembolso 1 ERROR
reembolso       2359
tarjet2a        1 ERROR
tarjeta 2491

Vemos que tenemos errores y campos vacíos en el tipo de pago."""


"""APARTADO 1.2"""

"""Creamos el directorio tmp/spooldir en mi home"""
mkdir -p tmp/spooldir

"""Mandamos los archivos de Caronte al directorio que hemos creado tmp/spooldir"""
scp luis.blanco@caronte:/var/log/httpd/access_log /home/luis.blanco/tmp/spooldir

"""Ocultamos el archivo para que no genere problemas Flume"""
mv access_log .access_log

"""Generamos el fichero spool-to-hdfs.properties en nuestra home"""
vim spool-to-hdfs.properties

# Definimos Sources, channels and sinks:
agent1.sources = source1
agent1.sinks = sink1
agent1.channels = channel1

# Conectamos el channel con la source y esta con el sink:
agent1.sources.source1.channels = channel1
agent1.sinks.sink1.channel = channel1

#Configuraremos el directorio de spooling que usaremos:
agent1.sources.source1.type = spooldir
agent1.sources.source1.spoolDir = /home/luis.blanco/tmp/spooldir/

#Definimos el tipo de channel (file, almacenamos en disco) y de sink (hdfs, se guardará en el path hdfs:///user/luis.blanco/import_logs/ con el sufijo .log):
agent1.channels.channel1.type = file

agent1.sinks.sink1.type = hdfs
agent1.sinks.sink1.hdfs.path = hdfs:///user/luis.blanco/import_logs/
agent1.sinks.sink1.hdfs.filePrefix = events
agent1.sinks.sink1.hdfs.fileSuffix = .log
agent1.sinks.sink1.hdfs.inUsePrefix = _
agent1.sinks.sink1.hdfs.fileType = DataStream

"""Ejecutamos nuestro agente Flume"""
flume-ng agent --conf-file spool-to-hdfs.properties --name agent1

"""Abrimos una nueva consola y hacemos visible el archivo access_log"""
mv .access_log access_log
#comprobamos que se ha terminado de ejecutar Flume al ver el archivo como COMPLETE

"""Usamos Pig para procesar los logs importados de HDFS y así generar un dato estructurado con
los datos que nos interesan de estos logs"""
pig
datos = LOAD 'import_logs/' USING TextLoader AS (line:chararray);
DUMP datos; #Comprobamos

datos_estructurados = FOREACH datos GENERATE FLATTEN(REGEX_EXTRACT_ALL(line,'(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}).*\\[(.*)\\]\\ "(.*) /.*')) AS
(IP: chararray, Fecha: chararray, Metodo: chararray);

DUMP datos_estructurados; #Comprobamos los datos

"""Almacenamos los datos en HDFS en el path /user/luis.blanco/tablas_hive/logs/ separando los campos por comas"""
STORE datos_estructurados INTO '/user/luis.blanco/tablas_hive/logs/' USING PigStorage(',');

"""Creamos la tabla externa logs en Hive"""
Hive
use luis_blanco;

CREATE EXTERNAL TABLE logs(
IP STRING,
Fecha STRING,
Metodo STRING)
COMMENT 'Esta es la tabla logs'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/luis.blanco/tablas_hive/logs/';

"""Comprobamos que se ha creado correctamente"""
SELECT * from logs;

"""APARTADO 1.3"""

"""Vamos a analizar los datos de las dos tablas:logs y bbdd"""
Hive
use luis_blanco
SELECT * FROM bbdd;
#Tenemos 5837 resultados
SELECT DISTINCT fecha FROM bbdd LIMIT 1; #2016-11-15 19:08:13
SELECT DISTINCT fecha FROM bbdd ORDER BY fecha DESC LIMIT 1; #2020-04-01 15:37:17
#Tenemos 5523 resultados, vemos que tenemos registros desde 2016-11-15 19:08:13 hasta 2020-04-01 15:37:17

SELECT * FROM logs;
SELECT DISTINCT SUBSTRING (fecha,4,8) from logs; 
#Tenemos 1753 resultados y observamos que todos los resultados son de marzo y abril de 2020, además, tenemos nulos.

"""Si queremos hacer un join con las tablas, tenemos que pasar el campo fecha de logs y bbdd al mismo formato,
teniendo que modificar los registros, para que esté en el mismo formato que en la tabla bbdd, este cambio lo realizamos
al crear la tabla auxiliar, junto con el resto de campos necesarios para tener la tabla maestra"""
04/Apr/2020:15:09:07 -0400 #Formato logs
2016-11-15 19:08:13 #Formato bbdd

"""Creamos una tabla auxiliar con los campos que necesitamos para crear la tabla maestra"""
CREATE TABLE tabla_aux as
SELECT logs.ip as IP, from_unixtime(unix_timestamp(logs.fecha,'dd/MMM/yyyy:HH:mm:ss z'),('yyyy-MM-dd HH:mm:ss'))as Date,
logs.metodo as Metodo, bbdd.tipo_pago as Tipo_Pago from logs LEFT JOIN bbdd ON (from_unixtime(unix_timestamp(logs.fecha,'dd/MMM/yyyy:HH:mm:ss z'),
('yyyy-MM-dd HH-mm-s')))= from_unixtime(unix_timestamp(bbdd.fecha,'yyyy-MM-dd HH:mm:ss'),
('yyyy-MM-dd HH-mm-s'));

"""Creamos la tabla maestra, teniendo en cuenta que sólo necesitamos la hora, no la fecha completa"""
CREATE TABLE tabla_maestra as
SELECT from_unixtime(unix_timestamp(Date,'yyyy-MM-dd HH:mm:ss'),('HH')) as Date, SUM(case when tipo_pago='tarjeta' then 1 else 0 end) as Pago,
SUM(case when tipo_pago='reembolso' then 1 else 0 end) as PAGO_CR,
SUM(case when metodo = 'GET' then 1 else 0 end) as NoCompra from tabla_aux GROUP BY from_unixtime(unix_timestamp(Date,'yyyy-MM-dd HH:mm:ss'),('HH'));

"""Ya tenemos nuestra tabla maestra con los datos que nos pide el ejercicio"""