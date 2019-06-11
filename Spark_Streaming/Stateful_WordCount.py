'''This program implements a stateful word counter.
It keep a running count of the words it sees in the stream so far
and updates it as it encounters new occurunces of the words.'''

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":

    #Create a local StreamingContext with batch interval of 10 seconds
    sc = SparkContext(appName="Stateful Network Word Count")
    ssc = StreamingContext(sc,10)
    
    ''' Checkpointing is done to be resilient to failures like system or JVM failures
    It is necessary for basic functioning when stateful transformations are used.'''
    ssc.checkpoint("checkpoint")
    
    
    #RDD with initial state (key, value) pairs, for example:-
    #initialStateRDD = sc.parallelize([(u'hello', 1), (u'world', 1)])
    initialStateRDD = sc.parallelize([])
    #I haven't added any initial pair as its not required here.
    
    #function to update the values
    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)
    
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    running_counts = lines.flatMap(lambda line: line.split(" "))\
                          .map(lambda word: (word, 1))\
                          .updateStateByKey(updateFunc, initialRDD=initialStateRDD)
    
    ''' The updateStateByKey applies the given function on the previous state of the key 
    and the new values for the key, to keep continous updates about new information.'''
    
    
    running_counts.pprint()
    
    ssc.start()
    ssc.awaitTermination()
