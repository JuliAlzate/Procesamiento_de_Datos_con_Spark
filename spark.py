from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count
from pyspark.sql.functions import avg, count, max, min
import time

# Inicializar Spark
spark = SparkSession.builder \
    .appName("Análisis de Casos COVID-19") \
    .getOrCreate()

# Ruta del archivo CSV en HDFS
file_path = "hdfs://localhost:9000/Tarea3/gt2j-8ykr.csv"

# Cargar el dataset
data_df = spark.read.csv(file_path, header=True, inferSchema=True)


# Análisis 1: Contar la cantidad de casos por departamento
print("Cantidad de casos por departamento:")
casos_por_departamento = data_df.groupBy("departamento_nom").count()
casos_por_departamento.show()
time.sleep(5)

# Análisis 2: Calcular la edad promedio de los casos
edad_promedio = data_df.select(avg("edad")).first()[0]
print(f"La edad promedio es: {edad_promedio}")

# Análisis 3: Contar el número de casos recuperados
casos_recuperados = data_df.filter(data_df.recuperado == "Sí").count()
print(f"Número de casos recuperados: {casos_recuperados}")

# Análisis 4: Contar casos por sexo
print("Cantidad de casos por sexo:")
casos_por_sexo = data_df.groupBy("sexo").count()
casos_por_sexo.show()
time.sleep(5)


# Análisis 5: Casos por fuente de contagio
print("Cantidad de casos por fuente de contagio:")
casos_por_fuente = data_df.groupBy("fuente_tipo_contagio").count()
casos_por_fuente.show()


# Análisis 6: Cantidad de casos activos, recuperados y fallecidos
print("Cantidad de casos por estado:")
casos_por_estado = data_df.groupBy("estado").count()
casos_por_estado.show()

# Análisis 7: Calcular la edad máxima y mínima
edad_maxima = data_df.select(max("edad")).first()[0]
edad_minima = data_df.select(min("edad")).first()[0]
print(f"La edad máxima es: {edad_maxima}")
print(f"La edad mínima es: {edad_minima}")
time.sleep(5)

# Análisis 8: Casos por ubicación del paciente (casa, hospital, UCI, etc.)
print("Cantidad de casos por ubicación del paciente:")
casos_por_ubicacion = data_df.groupBy("ubicacion").count()
casos_por_ubicacion.show()

# Análisis 9: Casos por grupo étnico
print("Cantidad de casos por grupo étnico:")
casos_por_etnia = data_df.groupBy("nom_grupo_").count()
casos_por_etnia.show()

# Análisis 10: Casos por tipo de recuperación (casa, hospital, etc.)
print("Cantidad de casos por tipo de recuperación:")
casos_por_recuperacion = data_df.groupBy("tipo_recuperacion").count()
casos_por_recuperacion.show()
time.sleep(5)

# Finalizar la sesión de Spark
spark.stop()


