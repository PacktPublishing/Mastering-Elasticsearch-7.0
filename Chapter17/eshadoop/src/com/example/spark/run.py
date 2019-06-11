from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from com.example.spark_ml.kmeans import create_anomaly_detection_model, find_anomalies
import pandas


# Create Spark Session
def create_spark_session():
    spark = SparkSession.builder.master("local").appName("anomalyDetection").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# Extract data from elasticsearch and select the fields for the anomaly detection
def extract_es_data(spark):
    reader = spark.read.format("org.elasticsearch.spark.sql").option("es.read.metadata", "true") \
                .option("es.nodes.wan.only", "true").option("es.port", "9200") \
                .option("es.net.ssl", "false").option("es.nodes", "http://localhost")
    df = reader.load("cf_rfem_hist_price")
    df.createTempView("view1")
    df2 = spark_session.sql("Select volume, changePercent, changeOverTime from view1")
    vec_assembler = VectorAssembler(inputCols=["changeOverTime", "changePercent", "volume"], outputCol="features")
    df_features = vec_assembler.transform(df2).select('features')
    return df_features


def convert_data_to_features(spark, data):
    schema = StructType([StructField("changeOverTime", FloatType()), StructField("changePercent", FloatType()),
                        StructField("volume", FloatType())])
    df_data = spark.createDataFrame(pandas.DataFrame(data), schema)
    vec_assembler = VectorAssembler(inputCols=["changeOverTime", "changePercent", "volume"], outputCol="features")
    df_features = vec_assembler.transform(df_data).select('features')
    return df_features


if __name__ == '__main__':
    spark_session = create_spark_session()
    es_data = extract_es_data(spark_session)
    centers = create_anomaly_detection_model(es_data)
    df_centers_features = convert_data_to_features(spark_session, centers)
    center_labels = find_anomalies(df_centers_features)
    print(center_labels)
