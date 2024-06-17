from pyspark.sql import SparkSession
import boto3
import json

# HÀM TẠO TEMPVIEW TỪ CÁC BẢNG TRÊN REDSHIFT
def create_tempview_from_redshift_query(redshift_dict, url, user, password): 
    spark = SparkSession.builder.getOrCreate()
    for view_name, query in redshift_dict.items():
        df = spark.read.format("jdbc") \
            .option("url", url) \
            .option("user", user) \
            .option("password", password) \
            .option("dbtable", query) \
            .load()
        df.createOrReplaceTempView(view_name)
        pass

def read_redshift_data_from_redshift_query_return_df(redshift_dict, url, user, password):
    spark = SparkSession.builder.getOrCreate()
    dataframes = {}
    for view_name, query in redshift_dict.items():
        df = spark.read.format("jdbc") \
            .option("url", url) \
            .option("user", user) \
            .option("password", password) \
            .option("dbtable", query) \
            .load()
        dataframes[view_name] = df
    return dataframes

# TRƯỜNG HỢP CẦN TRÍCH XUẤT NHIỀU BẢNG TỪ NGUỒN S3 TỪ
def create_tempview_from_csv_in_s3(dictionary):
    spark = SparkSession.builder.getOrCreate()
    for key, value in dictionary.items():
        stripped_value = value.strip() #cắt bỏ khoảng trắng trong value ở dictionary
        df = spark.read.option("header", "true").csv(stripped_value)
        df.createOrReplaceTempView(key.strip())  #tên temp view là key trong dict
        pass

def create_tempview_from_parquet_in_s3(dictionary):
    spark = SparkSession.builder.getOrCreate()
    for key, value in dictionary.items():
        stripped_value = value.strip() #cắt bỏ khoảng trắng trong value ở dictionary
        df = spark.read.parquet(stripped_value)
        df.createOrReplaceTempView(key.strip())
        pass

