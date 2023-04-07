from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive
df_clientes = spark.sql("Select * from desafio_curso.tbl_clientes")
df_regiao = spark.sql("Select * from desafio_curso.tbl_regiao")
df_endereco = spark.sql("Select * from desafio_curso.tbl_endereco")
df_vendas = spark.sql("Select * from desafio_curso.tbl_vendas")
df_divisao = spark.sql("Select * from desafio_curso.tbl_divisao")

# Espaço para tratar e juntar os campos e a criação do modelo dimensional

# criando o fato
df_ajuda = df_DIM_CLIENTES [['address_number', 'region_code', 'division', 'customerkey']]
df_FT_VENDAS = df_vendas.join(df_ajuda,df_vendas.customerkey == df_ajuda.customerkey,"inner").drop(df_ajuda.customerkey)

#criando as dimensões
df_DIM_CLIENTES = df_clientes.join(df_endereco,df_clientes.address_number ==  df_endereco.address_number,"inner").drop(df_endereco.address_number)

DIM Localidade:
df_juntar = df_clientes.join(df_divisao,df_clientes.division == df_divisao.division,"inner").drop(df_divisao.division)
df_juntar2 = df_juntar.join(df_regiao,df_juntar.region_code == df_regiao.region_code,"left").drop(df_regiao.region_code)
df_DIM_LOCALIDADES = df_juntar2[['region_code', 'region_name', 'division', 'division_name']]

DIM Tempo:
df_DIM_TEMPO = df_vendas [['Invoice_Date', 'Actual_Delivery_Date', 'Promised_Delivery_Date']]

FT_VENDAS:
df_ajuda = df_DIM_CLIENTES [['address_number', 'region_code', 'division', 'customerkey']]
df_FT_VENDAS = df_vendas.join(df_ajuda,df_vendas.customerkey == df_ajuda.customerkey,"inner").drop(df_ajuda.customerkey)

# função para salvar os dados


    def salvar_df(df, file):
    output = "/input/desafio_curso_indra/desafio_curso/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_curso_indra/desafio_curso/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

salvar_df(dim_clientes, 'dimclientes')