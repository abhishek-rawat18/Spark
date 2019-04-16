'''Fle watcher program using spark streaming to count
the words in a newly arriving ﬁle in a hadoop directory'''

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    
    conf = SparkConf().setAppName("Reading files from a directory")
    sc   = SparkContext(conf=conf)
    ssc  = StreamingContext(sc, 15)

    #Streams data from the hdfs directory given below
    lines = ssc.textFileStream("hdfs://localhost:9000/hw6")
    
    lines.pprint()
    
    #Split each line into words
    words = lines.flatMap(lambda line: line.split(" "))

    #Count each word in each batch
    pairs = words.map(lambda word: (word, 1))

    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    wordCounts.pprint()

    ssc.start()            #Start the computation
    ssc.awaitTermination() #Wait for the computation to terminate
