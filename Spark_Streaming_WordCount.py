'''Spark streaming program to connect to a socket on the local machine
using netcat. We write a stream of text on our netcat console.
This spark program receives the stream and produces a word count.'''

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys

if __name__ == "__main__":

    '''Create a local StreamingContext with two working thread
    and batch interval of 5 seconds'''
    
    sc=SparkContext(appName="Spark Streaming Word Count")
    ssc=StreamingContext(sc, 5)
    
    '''Create a DStream that will connect to hostname:port,
    like localhost:9999 . 
    DStream is the basic abstraction that represents a 
    continuous stream of data.'''
    
    lines=ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    #or we may write ssc.socketTextStream("localhost",9999)
    #if our port is localhost:9999
    '''This represents the stream of data that will be received from the data server'''
    
    
    #Splitting each line into words
    words=lines.flatMap(lambda line: line.split(" "))
    '''flatMap splits each line into multiple words and that 
    stream of words is represented by the variable named here as "words" '''
    
    # Count each word in each batch
    pairs=words.map(lambda word: (word, 1))
    wordCounts=pairs.reduceByKey(lambda x, y: x + y)
    
    wordCounts.pprint()
    
    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
