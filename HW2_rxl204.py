from pyspark import SparkContext
import csv
import sys

'''
hadoop fs -put *.csv .
spark-submit HW2_rxl204.py output_hw2
hadoop fs -ls output_hw2
hadoop fs -getmerge output_hw2 output.txt
'''

def parseRows(i, rows):
    if i == 0: 
        rows.next() # next(row)
    import csv
    rows = csv.reader(rows)
    for x in rows:
        yield x[0], x[7]


            
if __name__=='__main__':
    sc = SparkContext()

    INSPECTIONS = 'sys.argv[-2]'

    cuisine = sc.textFile(INSPECTIONS, use_unicode=False).cache()
    
r = (cuisine.mapPartitionsWithIndex(parseRows)
            .reduceByKey(lambda accum, y: y)
            .map(lambda (camis, cuisine): (cuisine, 1))
            .reduceByKey(lambda accum, n: accum + n)
            .sortBy(lambda x: -x[1])
            .saveAsTextFile(sys.argv[-1]))
