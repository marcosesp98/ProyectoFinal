# Databricks notebook source
import pathlib
import os
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
import pyspark.sql.functions as f
from pyspark.sql.types import *

spark=SparkSession.builder.appName("transformaciones").getOrCreate()

#importamos las librerias que vamos a usar

# COMMAND ----------

dbutils.fs.mount( 
    source = 'wasbs://input@storagesiglo21.blob.core.windows.net' ,
    mount_point = '/mnt/input',
    extra_configs = {'fs.azure.account.key.storagesiglo21.blob.core.windows.net':'Jbc3Ho1lMMkr2h0RFZLb7OKZB8SDXmv17sO490R0yzm4kd0m8Y0ZMddQ61YbeIg0O8JvMiSFIIJT+AStPH0+/A=='}) #esto es la conexion con el blob storage de salida

dbutils.fs.mount( 
    source = 'wasbs://output@datalakesiglo21.blob.core.windows.net' ,
    mount_point = '/mnt/output',
    extra_configs = {'fs.azure.account.key.datalakesiglo21.blob.core.windows.net':'7BabSWR4r7JRIBhoU4JEXycFEIL7HrBL2PW4mgc15Ojw24Ohm93vFMq58tLLax0TJ+gTDYyCZ3Ta+AStUMUILg=='}) #esto es la conexion con el datalake de salida

# COMMAND ----------

CategoriaSchema = StructType([
    StructField('Cod_Categoria', IntegerType(), True), 
    StructField('Categoria', StringType(), True)
])
FactMineSchema = StructType([
    StructField('TruckID', FloatType(),True),
    StructField('ProjectID',IntegerType(),True),
    StructField('OperatorID',IntegerType(),True),
    StructField('TotalOreMined',FloatType(),True),
    StructField('TotalWasted',FloatType(),True),
    StructField('Date',DateType(),True)
])
MineSchema = StructType([
    StructField('TruckID',IntegerType(),True),
    StructField('Truck',StringType(),True),
    StructField('ProjectID',IntegerType(),True),
    StructField('Country',StringType(),True),
    StructField('OperatorID',IntegerType(),True),
    StructField('FirstName',StringType(),True),
    StructField('LastName',StringType(),True),
    StructField('Age',IntegerType(),True),
    StructField('TotalOreMined',FloatType(),True),
    StructField('TotalWasted',FloatType(),True),
    StructField('Date',DateType(),True)
    
])
ProductoSchema = StructType([
    StructField('Cod_Producto',IntegerType(),True),
    StructField('Producto',StringType(),True),
    StructField('Cod_SubCategoria',IntegerType(),True),
    StructField('Color',StringType(),True)
])
SubCategoriaSchema = StructType([
    StructField('Cod_SubCategoria',IntegerType(),True),
    StructField('SubCategoria',StringType(),True),
    StructField('Cod_Categoria',IntegerType(),True)
])
VentasInternetSchema = StructType([
    StructField('Cod_Producto',IntegerType(),True),
    StructField('Cod_Cliente',IntegerType(),True),
    StructField('Cod_Territorio',IntegerType(),True),
    StructField('NumeroOrden',IntegerType(),True),
    StructField('Cantidad',IntegerType(),True),
    StructField('PrecioUnitario',FloatType(),True),
    StructField('CostoUnitario',FloatType(),True),
    StructField('Impuesto',FloatType(),True),
    StructField('Flete',StringType(),True),
    StructField('FechaOrden',DateType(),True),
    StructField('FechaEnvio',DateType(),True),
    StructField('FechaVencimiento',DateType(),True),
    StructField('Cod_Promocion',IntegerType(),True)
])
#creacion de esquemas para los csv

# COMMAND ----------

dfCategorias=spark.read.csv('/mnt/input/Categoria.csv', header=True, inferSchema=True)
dfFactMine=spark.read.csv("/mnt/input/FactMine.csv", header=True, inferSchema=True)
dfMine=spark.read.csv("/mnt/input/Mine.csv", header=True, inferSchema=True)
dfProducto=spark.read.csv("/mnt/input/Producto.csv", header=True, inferSchema=True)
dfSubcategoria=spark.read.csv("/mnt/input/SubCategoria.csv", header=True, inferSchema=True)
dfVentasInternet=spark.read.csv("/mnt/input/VentasInternet.csv", header=True, inferSchema=True)

#importacion de los csv

# COMMAND ----------

dfTransform1 = dfCategorias.withColumnRenamed("Categoria", "Nombre_Categoria")
dfTransform2 = dfFactMine.groupBy().sum("TotalOreMined").withColumnRenamed("sum(TotalOreMined)","HistoricalSumOreMined")
dfTransform3 = dfMine.select('Country','FirstName','LastName','Age')
dfTransform4 = dfMine.groupBy('Country').sum('TotalWasted').withColumnRenamed('sum(TotalWasted)','TotalWastedPerCountry')
dfTransform5 = dfProducto.select(countDistinct('Cod_Producto')).withColumnRenamed('count(DISTINCT Cod_Producto)','cantidad_de_productos')
dfTransform6 = dfProducto.orderBy('Cod_Producto', ascending=False)
dfTransform7 = dfSubcategoria.filter(dfSubcategoria.Cod_Categoria==3)
dfTransform8 = dfVentasInternet.withColumn('Ingresos_Netos', (dfVentasInternet.Cantidad*dfVentasInternet.PrecioUnitario)-dfVentasInternet.CostoUnitario)
dfTransform9 = dfTransform8.groupBy('Cod_Producto').agg(f.mean('Ingresos_Netos'),f.sum('Ingresos_Netos')).orderBy('Cod_Producto',ascending = True).withColumnRenamed('avg(Ingresos_Netos)','Promedio_IngresosNetos').withColumnRenamed('sum(Ingresos_Netos)','Suma_ingresos_netos')

#aplicamos las correspondientes transformaciones

# COMMAND ----------

for i in range(1,10):
    exec("dfTransform%s.write.format('csv').mode('append').options(header='true').save('/mnt/output/temp/transform%s')" % (i,i))

# COMMAND ----------

#listamos los directorios de la carpeta temporal del datalake
lista_directorios=dbutils.fs.ls('/mnt/output/temp/')

#creo un array que contiene arrays de directorios
lista_de_sublistas=[dbutils.fs.ls(directorio.path) for directorio in lista_directorios] #crea una lista con tantas listas como directorios
archivos_desempaquetados = [archivo for lista in lista_de_sublistas for archivo in lista] #aplanamos todo en una misma lista

#movemos los archivos finales a una carpeta
for archivo in archivos_desempaquetados:
    path_instance=pathlib.PurePosixPath(archivo.path)
    if re.search('part.+', archivo.name): 
        dbutils.fs.mv(archivo.path,'/mnt/output/%s.csv' % path_instance.parents[0].name)
        
#borramos las carpetas residuales
for i in range(1,10):
    exec("dbutils.fs.rm('mnt/output/temp/transform%s',recurse=True)" % i)

