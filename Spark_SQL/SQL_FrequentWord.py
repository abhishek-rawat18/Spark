'''This program illustrates the use of spark SQL
to query the stream we receive from the socket.
It also illustrate how we can perform Dataframe operations
on the received stream. Here I've performed the operation of
selecting the top 2 frequent words in an input line.'''

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

'''The entry point into all functionality in Spark is the SparkSession class
   which is built below using ".builder" '''

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


if __name__ == "__main__":

    host, port = sys.argv[1:] 
    #this will correspond to localhost 9999 for example.
    
    sc = SparkContext(appName="Python Sql Network Word Count")
    ssc = StreamingContext(sc, 1)

    '''Create a socket stream on target ip:port and count the words in input stream of text''' 
    lines = ssc.socketTextStream(host, int(port))
    words = lines.flatMap(lambda line: line.split(" "))

    # Convert RDDs of the words DStream to DataFrame and run SQL query
    def process(time, rdd):
        print("========= %s =========" % str(time))

        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda w: Row(word=w))
            wordsDataFrame = spark.createDataFrame(rowRdd)

            # Creates a temporary view using the DataFrame.
            wordsDataFrame.createOrReplaceTempView("words")

            # The sql query we want to perform
            sqlOperation = spark.sql("SELECT word FROM words GROUP BY word ORDER BY COUNT(*) DESC LIMIT 2")
            #This query selects and displays the top 2 frequent words in an input line.

            '''Note that if word count was needed we could have written
            spark.sql("select word, count(*) as total from words group by word")
            instead of the above'''
            
            sqlOperation.show()
            
        except:
            pass

    words.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
