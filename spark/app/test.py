
import sys
from pyspark.sql import SparkSession, functions
from preprocess_module import preprocess

# Create spark session
spark = (SparkSession
        .builder
        .getOrCreate())

sc = spark.sparkContext
sc.setLogLevel("WARN")

df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
    .csv('/home/jovyan/work/resources/data/dataset/raw.csv')
    
testData = preprocess._fully_preprocess(df)

from pyspark.ml import PipelineModel
pipelineModel = PipelineModel.load("/home/jovyan/work/notebooks/model/DT_5d_5p")
test = pipelineModel.transform(testData)



