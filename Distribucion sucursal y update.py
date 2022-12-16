# Databricks notebook source


# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import os
import json
import pyodbc
spark=SparkSession.builder.appName("transformaciones").getOrCreate()

# COMMAND ----------

datalake='datalakesiglo21'
container='getsfront'
AzureSQL='sqlsiglo211'
AccesKey='YIDLPbl6Ec19gAfd/KOMeIpJSyjn5A+8qnHeoxksA2dIBz+THJiMDnyYJZcQeeEEtQiUqcVh8llK+ASt6D4S9w=='
bacpac='dbRetail'
user='server'
pss='Test1234'

# COMMAND ----------

dbutils.fs.mount(
    source = 'wasbs://'+container+'@'+datalake+'.blob.core.windows.net',
    mount_point = '/mnt/'+container,
    extra_configs = {'fs.azure.account.key.'+datalake+'.blob.core.windows.net':AccesKey}
)
#creo el punto de montaje con el datalake

# COMMAND ----------

jdbcHostname = AzureSQL+".database.windows.net"
jdbcPort = 1433
jdbcDatabase = bacpac
jdbcUsername = user
jdbcPassword = "Test1234"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

#conexion al azure sql

# COMMAND ----------

jsonProductos=spark.read.option('multiline','true').json('/mnt/putsfront/salida.json')
jsonProductos.show()
#LEO EL JSON DEL PROVEEDOR ^^^^^^

jsonProductospd=jsonProductos.toPandas()
columnas_json=jsonProductospd.to_numpy().transpose().tolist()
lista_cod_producto=[int(i) for i in columnas_json[0]]
lista_stock=[int(i) for i in columnas_json[1]]
#CREO LISTAS CON LAS COLUMNAS DEL DATAFRAME ^^^^^^
#LAS VOY A METER A UNA FUNCION MAP PARA QUE ME PRODUZCA LOS DATAFRAME A GUARDAR ^^^^

# COMMAND ----------

dfVentasInternet=spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable", "dbo.ventasinternet").load()
#INVOCO A LA TABLA VENTAS INTERNET DE SQL. DE ACA VOY A HACER UNA CONSULTA PARA CONSEGUIR EL STOCK OPTIMO ^^^^^^
dfStockProductos=spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable", "dbo.stockproductos").load()


#OBSERVACION
dfVentasInternet.createOrReplaceTempView('vista_tabla')
dfVentasInternet2=spark.sql('select sum(cantidad) as suma, cod_producto, cod_territorio, month(fechaenvio) as mes from vista_tabla group by month(fechaenvio), cod_producto, cod_territorio')
dfVentasInternet2.createOrReplaceTempView('dfVentasInternet2')
dfStockOptimo=spark.sql('select cod_producto, cod_territorio, avg(suma) as cantidad from dfVentasInternet2 group by cod_producto, cod_territorio order by cod_producto, cod_territorio')
#creo tabla de stock optimo, que va a usarse de referencia en la distribucion de stock




dfStockOptimo=dfStockOptimo.withColumn('cantidad', dfStockOptimo["cantidad"].cast('integer'))
dfStockOptimo.show()
#uso finalmente una instancia del dataframe stock optimo para referenciarla en la siguiente funcion
'''NOTA: esta celda puede ser innecesariamente densa, la intencion de todo esto es obtener solamente la tabla stock, pero en databricks no consegui trabajar con 
la sentencia CREATE VIEW de sql "OBSERVACION", si se pudiera hacer eso el codigo se puede optimizar'''

# COMMAND ----------

#con esta funcion, aplico la distribucion de stock a las sucursales usando como referencia el dataframe "stock"


def distribuir_stock(cod_prod,cantidad):
    
    #cod_producto es el producto en si mismo
    #la cantidad es el stock que agregas de ese producto
    #filtrado de tabla stock, quiero crearme un porcentaje de distribucion en tiempo real del codigo producto pedido
    df=dfStockOptimo.filter(dfStockOptimo['cod_producto']==cod_prod)
    
    df1=dfStockProductos.filter(dfStockProductos['cod_producto']==cod_prod)
    #CHEQUEO SI HAY STOCK EXISTENTE
    
    #-------------------------------------------------------------------------------------------------------------------------------
    
    #CASO 1: SI EL PRODUCTO TIENE REFERENCIA DE STOCK
    if df.count() > 0: #si el dataframe es no nulo
        df_auxiliar=df.select('cod_territorio','cantidad')
        #transformacion a lista y parse a int
        df_auxiliarDePandas=df_auxiliar.toPandas()
        arrays=df_auxiliarDePandas.to_numpy().transpose().tolist() #esto sirve para usar las columnas como listas
        
        #arrays es una lista de listas, la primera son las sucursales que tienen el producto, la segunda es el stock ideal
        sucursales=[int(i) for i in arrays[0]]
        referencia_stock=[int(i) for i in arrays[1]]
        
        suma_cantidad=sum(referencia_stock)
        todas_las_sucursales=[1,2,3,4,5,6,7,8,9,10]
        
        #cuando la cantidad de sucursales que registran ventas sea menor a 10, ejecuto esto que me completa el n° de sucursales a 10
        if len(sucursales)<10: 
            [todas_las_sucursales.remove(i) for i in sucursales] #luego de este bucle, del total de sucursales solo quedan las que no estaban inicialmente
            for j in todas_las_sucursales:
                referencia_stock.append(0) #las sucursales que no estaban inicialmente les agrego stock cero
            porcentaje=list(map(lambda x: (x/suma_cantidad)*100,referencia_stock))
            sucursales=sucursales+todas_las_sucursales
        
        #si las sucursales estan bien, simplemente aplico el porcentaje de distribucion
        else:
            porcentaje=list(map(lambda x: (x/suma_cantidad)*100,referencia_stock))
        
        #si la cantidad de stock es mayor a la cantidad de sucursales (10)
        if cantidad>10:
            stock_sucursal_auxiliar_1=[1,1,1,1,1,1,1,1,1,1]
            #la primera reparticion de stock va a ser uniforme, esto hago arriba ^^^^
            

            stock_sucursal_auxiliar_2=list(map(lambda x: int((cantidad-10)*(x/100)),porcentaje))
            #luego de la primera reparticion, continuo pero ahora con los porcentajes de venta por producto de cada sucursal ^^^^
            
            stock_sucursal=list(map(lambda x,y: x+y,stock_sucursal_auxiliar_1,stock_sucursal_auxiliar_2))
            #unifico las dos reparticiones
            
            diferencia=cantidad-sum(stock_sucursal)
            #al haber usado int en stock_sucursal_auxiliar_2 me redondea la cantidad para abajo (porcentajes usa decimales), el excedente se reparte

            for i in range(len(stock_sucursal)):
                x=stock_sucursal[i]+1
                stock_sucursal.pop(i)
                stock_sucursal.insert(i,x)
                diferencia-=1
                if diferencia==0:
                    break
        
        #si la cantidad de stock es menor a la cantidad de sucursales, reparto de a 1 en las primeras sucursales
        else:
            stock_sucursal=[0,0,0,0,0,0,0,0,0,0]
            for z in range(len(stock_sucursal)):
                x=stock_sucursal[z]+1
                stock_sucursal.pop(z)
                stock_sucursal.insert(z,x)
                cantidad-=1
                if cantidad==0:
                    break 
        
        if df1.count()>0:
            df_auxiliar2=df1.select('StockReal')
            df_auxiliarDePandas2=df_auxiliar2.toPandas()
            arrays1=df_auxiliarDePandas2.to_numpy().transpose().tolist() #esto sirve para usar las columnas como listas
            stock_sucursal=list(map(lambda x,y: x+y,stock_sucursal,arrays1))
            #ACA SUMO EL STOCK QUE AGREGO CON EL EXISTENTE 
                                
            
    #-------------------------------------------------------------------------------------------------------------------------------
    
    
    #CASO 2: si el dataframe stock no tiene registros para crear la distribucion del stock, aplico la distribucion del total de ventas historico
    else:
        porcentajes=[14.639358483134,0.040200579734,0.031737299790,19.945835008022,0.059242959608,11.876097581418,9.438672957546,9.614286016384,22.745064849500,11.609504263182]
        referencia_stock=[0,0,0,0,0,0,0,0,0,0]
        sucursales=[1,2,3,4,5,6,7,8,9,10]
        if cantidad>10:

            stock_sucursal1=[1,1,1,1,1,1,1,1,1,1]
            stock_sucursal2=list(map(lambda x: int((cantidad-10)*(x/100)),porcentajes))
            stock_sucursal=list(map(lambda x,y: x+y,stock_sucursal1,stock_sucursal2))
            diferencia=cantidad-sum(stock_sucursal)

            for i in range(len(stock_sucursal)):
                x=stock_sucursal[i]+1
                stock_sucursal.pop(i)
                stock_sucursal.insert(i,x)
                diferencia-=1
                if diferencia==0:
                    break
        else:
            stock_sucursal=[0,0,0,0,0,0,0,0,0,0]
            for i in range(len(stock_sucursal)):
                x=stock_sucursal[i]+1
                stock_sucursal.pop(i)
                stock_sucursal.insert(i,x)
                cantidad-=1
                if cantidad==0:
                    break  
                                
        if df1.count()>0:
            df_auxiliar2=df1.select('StockReal')
            df_auxiliarDePandas2=df_auxiliar2.toPandas()
            arrays1=df_auxiliarDePandas2.to_numpy().transpose().tolist() 
            print(arrays1)
            stock_sucursal=list(map(lambda x,y: x+y,stock_sucursal,arrays1[0]))
            #ACA SUMO EL STOCK QUE AGREGO CON EL EXISTENTE 
            
    datos={'Cod_Producto':[cod_prod,cod_prod,cod_prod,cod_prod,cod_prod,cod_prod,cod_prod,cod_prod,cod_prod,cod_prod],
   'Cod_Sucursal':sucursales,
   'StockOptimo':referencia_stock,
   'StockReal':stock_sucursal}
    df=pd.DataFrame(datos)
    return spark.createDataFrame(df)


# COMMAND ----------

#esta operacion lo que hace es usar la funcion distribuir stock y las listas del json cargados del datalake, y con las variables creadas hago la distribucion automatica
lista_dataframes=list(map(distribuir_stock,lista_cod_producto,lista_stock))


# COMMAND ----------

def upsertAzureSQL(df, azureStagingTable, azureSqlTargetTable, lookupColumns, deltaName):
    targetTableAlias="stage"
    stagingTableAlias="stockproductos"
    
    dfColumns=str(df.columns)
    dfColumns=(((dfColumns.replace("'","")).replace("[","")).replace("]","")).replace(" ","")
    
    mergeStatement= "MERGE "+azureSqlTargetTable+" as "+targetTableAlias+" USING "+azureStagingTable+" as "+stagingTableAlias+" ON ("
    
    if (lookupColumns is not None or lookupColumns is len(lookupColumns)>0):
        uniqueCols=lookupColumns.split("|")
        lookupStatement=""
        for lookupCol in uniqueCols:
            lookupStatement=lookupStatement+targetTableAlias+"."+lookupCol+" = "+stagingTableAlias+"."+lookupCol+ " and "
        
    if deltaName is not None and len(deltaName)>0:
        updateStatement=lookupStatement+stagingTableAlias+"."+deltaName+" >= "+targetTableAlias+"."+deltaName
    else:
        remove="and"
        reverse_remove=remove[::-1]
        updateStatement=lookupStatement[::-1].replace(reverse_remove,"",1)[::-1]
    if deltaName is not None and len(deltaName)>0:
        updateStatement=updateStatement+" and "+targetTableAlias+"."+deltaName+" < "+stagingTableAlias+"."+deltaName
    updateStatement=updateStatement+") WHEN MATCHED THEN UPDATE SET "
    updateColumns=dfColumns.split(",")
    for lookupCol in updateColumns:
        updateStatement=updateStatement+targetTableAlias+"."+lookupCol+" = "+stagingTableAlias+"."+lookupCol+", "
    remove=","
    reverse_remove=remove[::-1]
    updateStatement=updateStatement[::-1].replace(reverse_remove,"",1)[::-1]+";"
    
    updateStatement=mergeStatement+updateStatement
    
    remove="and"
    reverse_remove=remove[::-1]
    insertLookupStatement=lookupStatement[::-1].replace(reverse_remove,"",1)[::-1]+")"
    
    insertStatement=insertLookupStatement+" WHEN NOT MATCHED BY TARGET THEN INSERT ("+dfColumns.replace(",", ", ")+") VALUES ("
    for lookupCol in updateColumns:
        insertStatement=insertStatement+stagingTableAlias+"."+lookupCol+", "
    remove=","
    reverse_remove=remove[::-1]
    insertStatement=insertStatement[::-1].replace(reverse_remove,"",1)[::-1]+");"
    insertStatement=mergeStatement+insertStatement
    finalStatement=updateStatement+insertStatement
    
    df.write.format("jdbc").option("url", jdbcUrl).option("dbtable", azureStagingTable).option("truncate","true").option("schemaCheckEnabled","false").mode("overwrite").save()
    
    conn=pyodbc.connect('DRIVER={ODBC Driver 17 for Sql Server};'
                        'SERVER='+AzureSQL+'.database.windows.net;'
                        'DATABASE='+bacpac+';UID='+user+';'
                        'PWD='+pss)
    cursor=conn.cursor()
    conn.autocommit=True
    cursor.execute(finalStatement)
    conn.close()
    
    return finalStatement

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
# MAGIC apt-get update
# MAGIC ACCEPT_EULA=Y apt-get install msodbcsql17
# MAGIC apt-get -y install unixodbc-dev
# MAGIC sudo apt-get install python3-pip -y
# MAGIC pip3 install --upgrade pyodbc

# COMMAND ----------

list(map(upsertAzureSQL,lista_dataframes,
         ["Stage" for i in range(len(lista_stock))],
         ["stockproductos" for i in range(len(lista_stock))],
         ["Cod_Producto|Cod_Sucursal" for i in range(len(lista_stock))],
         [None for i in range(len(lista_stock))]))

# COMMAND ----------


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
    

# COMMAND ----------

df2=lista_dataframes[0].toPandas()
print(df2)
stockProductosDatalake=spark.read.json('/mnt/getsfront/StockProductos')
stockProductosDatalakePandas=stockProductosDatalake.toPandas()
display(stockProductosDatalakePandas)


# COMMAND ----------



stockProductosDatalakePandas.set_index(["Cod_Producto","Cod_Sucursal"],inplace=True)
stockProductosDatalakePandas.update(df2.set_index(["Cod_Producto","Cod_Sucursal"]))
print(stockProductosDatalakePandas)
stockProductosDatalakePandas.reset_index(drop=True,inplace=True)

print(stockProductosDatalakePandas.columns)

# COMMAND ----------

display(dfspark)

# COMMAND ----------

stockProductosDatalakePandas.set_index("Cod_Producto",inplace=True)
stockProductosDatalakePandas.update(df2.set_index('Cod_Producto',inplace=True))
stockProductosDatalakePandas.reset_index
dfspark=createDataFrame(stockProductosDatalakePandas)
display(dfspark)

# COMMAND ----------

transformacion_almacenar(dfProducto,container,"Productos")


# COMMAND ----------


