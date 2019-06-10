from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from com.example.spark_ml.kmeans import create_anomaly_detection_model, find_anomalies
import pandas


# Create sql context
def create_sql_context():
    conf = SparkConf().setAppName("anomalyDetection")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("INFO")
    return SQLContext(sc)


# Extract data from elasticsearch and select the fields for the anomaly detection
def extract_es_data(sql_context):
    reader = sql_context.read.format("org.elasticsearch.spark.sql").option("es.read.metadata", "true") \
                .option("es.nodes.wan.only", "true").option("es.port", "9200") \
                .option("es.net.ssl", "false").option("es.nodes", "http://localhost")
    df = reader.load("cf_rfem_hist_price")
    df.createTempView("view1")
    df2 = sql_context.sql("Select volume, changePercent, changeOverTime from view1")
    vec_assembler = VectorAssembler(inputCols=["changeOverTime", "changePercent", "volume"], outputCol="features")
    df_features = vec_assembler.transform(df2).select('features')
    return df_features


if __name__ == '__main__':
        sqlContext = create_sql_context()
        df_data = extract_es_data(sqlContext)
        centers = create_anomaly_detection_model(df_data)
        cSchema = StructType([StructField("changeOverTime", FloatType()), StructField("changePercent", FloatType()),
                              StructField("volume", FloatType())])
        df_centers = sqlContext.createDataFrame(pandas.DataFrame(centers), cSchema)
        vecAssembler = VectorAssembler(inputCols=["changeOverTime", "changePercent", "volume"], outputCol="features")
        df_centers_features = vecAssembler.transform(df_centers).select('features')
        center_labels = find_anomalies(df_centers_features)
        print(center_labels)

