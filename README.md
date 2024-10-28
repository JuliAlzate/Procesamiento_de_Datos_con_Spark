# Procesamiento_de_Datos_con_Spark

para trabajar con spark streaming 
  Instalamos mediante PIP la librería de Python Kafka
  pip install kafka-python

  Descargue, descomprima y mueva de carpeta Apache Kafka
  wget https://downloads.apache.org/kafka/3.6.2/kafka_2.13-3.6.2.tgz
  tar -xzf kafka_2.13-3.6.2.tgz

  
  Iniciamos el servidor ZooKeeper:
 sudo /opt/Kafka/bin/zookeeper-server-start.sh 
/opt/Kafka/config/zookeeper.properties &

 Iniciamos el servidor Kafka:
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &

 Creamos un tema (topic) de Kafka, el tema se llamará sensor_data
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --
replication-factor 1 --partitions 1 --topic sensor_data
 
 Implementación del productor(producer) de Kafka
Creamos un archivo llamado kafka_producer.py
nano kafka_producer.py
python3 kafka_producer.py
