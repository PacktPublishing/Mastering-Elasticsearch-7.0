from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from com.example.spark_ml.kmeans import anomaly_detection, find_anomalies
import pandas

def extractData(sqlContext):

        reader = sqlContext.read.format("org.elasticsearch.spark.sql").option("es.read.metadata", "true") \
                .option("es.nodes.wan.only", "true").option("es.port", "9200") \
                .option("es.net.ssl", "false").option("es.nodes", "http://localhost")
        df = reader.load("cf_rfem_hist_price")
        df.createTempView("view1")
        df2 = sqlContext.sql("Select volume, changePercent, changeOverTime from view1")
        vecAssembler = VectorAssembler(inputCols=["changeOverTime", "changePercent", "volume"], outputCol="features")
        df_kmeans = vecAssembler.transform(df2).select('features')
        return df_kmeans

if __name__ == '__main__':
        conf = SparkConf().setAppName("myApp")
        sc = SparkContext(conf=conf)
        sc.setLogLevel("INFO")
        sqlContext = SQLContext(sc)
        df_kmeans = extractData(sqlContext)
        centers = anomaly_detection(df_kmeans)
        cSchema = StructType([StructField("changeOverTime", FloatType()), StructField("changePercent", FloatType()),
                              StructField("volume", FloatType())])
        df_centers = sqlContext.createDataFrame(pandas.DataFrame(centers), cSchema)
        vecAssembler = VectorAssembler(inputCols=["changeOverTime", "changePercent", "volume"], outputCol="features")
        df_centers_features = vecAssembler.transform(df_centers).select('features')
        label = find_anomalies(df_centers_features)


