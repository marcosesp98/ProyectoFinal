# Databricks notebook source
datalake='datalakesiglo21'
containerdl='getsfront'
AzureSQL='sqlsiglo211'
AccessKey='YIDLPbl6Ec19gAfd/KOMeIpJSyjn5A+8qnHeoxksA2dIBz+THJiMDnyYJZcQeeEEtQiUqcVh8llK+ASt6D4S9w=='
bacpac='dbRetail'
user='server'
pss='Test1234'

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
import os
import json
from pyspark.sql.functions import countDistinct,col,mean,sum
spark=SparkSession.builder.appName("transformaciones").getOrCreate()

# COMMAND ----------

def montar_almacenamiento(storage,container,accesskey):
    #se crean carpetas sobre las cuales se montan los container
    dbutils.fs.mkdirs('/mnt/'+container)
    #se montan los container en las carpetas
    dbutils.fs.mount(
    source = 'wasbs://'+container+'@'+storage+'.blob.core.windows.net' ,
    mount_point = '/mnt/'+container,
    extra_configs = {'fs.azure.account.key.'+storage+'.blob.core.windows.net':accesskey})

#aca guarda la variable transformacion dentro de la carpeta container en el archivo nombretransformacion
def transformacion_almacenar(transformacion,container,nombretransformacion):
    #se guarda la transformacion en la carpeta temporal
    transformacion.coalesce(1).write.mode("overwrite").format("json").save("dbfs:/mnt/"+container+"/temp")
    #se guarda solo el archivo que tiene las transformaciones en si
    files = dbutils.fs.ls("/mnt/"+container+"/temp")
    output_file = [x for x in files if x.name.startswith("part-")]
    filename=output_file[0].name
    dbutils.fs.mv("dbfs:/mnt/"+container+"/temp/"+filename,"dbfs:/mnt/"+container+"/"+nombretransformacion)
    #se borra archivos temporales
    tempfiles = dbutils.fs.ls("dbfs:/mnt/"+container+"/temp/")
    for x in tempfiles:
        dbutils.fs.rm("dbfs:/mnt/"+container+"/temp/"+x.name,recurse=True)

    with open("/dbfs/mnt/"+container+"/"+nombretransformacion,"r") as arch:
        lines = [json.loads(line.rstrip()) for line in arch]
    arch = open("/dbfs/mnt/"+container+"/"+nombretransformacion,"w")
    arch.write(str(lines).replace("\'","\""))
    arch.close()
    

#se monta datalake
montar_almacenamiento(datalake,containerdl,AccessKey)

#se crea carpeta temporal
dbutils.fs.mkdirs('/mnt/'+containerdl+'/temp')

# COMMAND ----------

dfCategorias=spark.read.json('/mnt/getsfront/Categoria')
dfProducto=spark.read.json('/mnt/getsfront/Producto')
dfSubCategoria=spark.read.json('/mnt/getsfront/SubCategoria')
dfSucursales=spark.read.json('/mnt/getsfront/Sucursales')

# COMMAND ----------

# MAGIC %md
# MAGIC --transformacion 1
# MAGIC 
# MAGIC select distinct Producto.Producto,Categoria.Categoria,SubCategoria.SubCategoria,StockProductos2.StockTotal 
# MAGIC 
# MAGIC from (select Cod_Producto,SUM(StockProductos.Cantidad) as StockTotal from StockProductos
# MAGIC group by StockProductos.Cod_Producto) as StockProductos2
# MAGIC 
# MAGIC inner join Producto on StockProductos2.Cod_Producto = Producto.Cod_Producto
# MAGIC 
# MAGIC inner join SubCategoria on Producto.Cod_SubCategoria = SubCategoria.Cod_SubCategoria
# MAGIC 
# MAGIC inner join Categoria on SubCategoria.Cod_Categoria = Categoria.Cod_Categoria

# COMMAND ----------

#codigo para crear el json "cuando hay stock"
dfStockTotal = dfStockProductos.groupBy('Cod_Producto').sum('StockReal').withColumnRenamed("sum(StockReal)","StockTotal")
joinproducto = dfStockTotal.join(dfProducto,dfStockTotal.Cod_Producto == dfProducto.Cod_Producto,"inner")
joinsubcategoria =joinproducto.join(dfSubCategoria,joinproducto.Cod_SubCategoria == dfSubCategoria.Cod_SubCategoria,"inner")
joincategoria = joinsubcategoria.join(dfCategoria,joinsubcategoria.Cod_Categoria == dfCategoria.Cod_Categoria,"inner")
transformacion1 = joincategoria.select(dfProducto.Cod_Producto,col("Producto"),col("StockTotal"),col("Categoria"),col("SubCategoria")).distinct()
transformacion_almacenar(transformacion1,containerdl,"cuandoHayStock.json")

# COMMAND ----------

# MAGIC %md
# MAGIC --transformacion 2 .de este codigo saque la salida cuando hay sucursales
# MAGIC 
# MAGIC SELECT distinct Producto.Producto,Categoria.Categoria,SubCategoria.SubCategoria,StockProductos.Cantidad,Sucursales.Sucursal FROM StockProductos
# MAGIC 
# MAGIC INNER JOIN Producto ON StockProductos.Cod_Producto = Producto.Cod_Producto
# MAGIC 
# MAGIC INNER JOIN Sucursales ON StockProductos.Cod_Sucursal = Sucursales.Cod_Sucursal
# MAGIC 
# MAGIC INNER JOIN SubCategoria ON Producto.Cod_SubCategoria = SubCategoria.Cod_SubCategoria
# MAGIC 
# MAGIC INNER JOIN Categoria ON SubCategoria.Cod_Categoria =Categoria.Cod_Categoria
# MAGIC 
# MAGIC WHERE NOT Sucursales.Sucursal='NA';

# COMMAND ----------

#codigo para crear el json "cuando NO!! hay stock"
joinproducto = dfStockProductos.join(dfProducto,dfStockProductos.Cod_Producto == dfProducto.Cod_Producto,"inner")
joinsucursales = joinproducto.join(dfSucursales,dfStockProductos.Cod_Sucursal == dfSucursales.Cod_Sucursal)
joinsubcategoria = joinsucursales.join(dfSubCategoria,joinproducto.Cod_SubCategoria == dfSubCategoria.Cod_SubCategoria,"inner")
joincategoria = joinsubcategoria.join(dfCategoria,joinsubcategoria.Cod_Categoria == dfCategoria.Cod_Categoria,"inner")
transformacion2 = joincategoria.select(dfProducto.Cod_Producto,col("Producto"),col("StockReal"),col("Sucursal")).distinct()
transformacion_almacenar(transformacion2,containerdl,"cuandoNoHayStock.json")


# COMMAND ----------

#se borra las carpetas temporales que no se van a usar mas
dbutils.fs.rm("dbfs:/mnt/"+containerdl+"/temp/",recurse=True)
#es necesario desmontar los puntos de montaje despues de ser usados
dbutils.fs.unmount('/mnt/'+containerdl)
