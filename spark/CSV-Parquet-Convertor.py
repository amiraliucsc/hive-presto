from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *


if __name__ == "__main__":
    sc = SparkContext(appName="CSV2Parquet")
    sqlContext = SQLContext(sc)
    
    schema = StructType([
            StructField("gender", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("count", IntegerType(), True)])
    
   
    states = ['IN','IL','KS','SC','HI','GA','SD','CO','NH','MS','MD','UT','LA','ME',
        'WI','NJ','AR','NY','MT','OK','MA','NM','WY','OH','OR','NV','TX','TN','AZ',
        'MN','WA','WV','NC','MO','AL','VA','CA','CT','AK','ND','VT','MI','NE','KY',
        'ID','DC','IA','FL','PA','RI','DE']
    for state in states: 
        rdd = sc.textFile("/data/csv/"+state+".csv") \
            .map(lambda line: line.split(",")) \
            .map(lambda line: (line[0], int(line[1]), line[2], int(line[3])))
        df = sqlContext.createDataFrame(rdd, schema)
        df.coalesce(1).write.parquet('/data/output/'+state)
