#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
import os

# sparkAppName = os.environ.get('SPARK_APP_NAME')
# sparkCluster = os.environ.get('SPARK_CLUSTER')

sparkAppName = 'businessApp'
# sparkCluster = 'local'


# conf = SparkConf().setAppName(sparkAppName).setMaster(sparkCluster)
conf = SparkConf().setAppName(sparkAppName)
sc = SparkContext(conf=conf)

def extract_city_names(x):
    city=x.split('"')[5]
    return city

whole_file_rdd = sc.textFile('gs://<your-cloud-storage-bucket>/businesses_plus.csv')

only_city_rdd = whole_file_rdd.map(extract_city_names)
map_cities_rdd = only_city_rdd.map(lambda x: (x,1))
business_by_cities_rdd = map_cities_rdd.reduceByKey(lambda x,y: x + y)

swap_business_rdd = business_by_cities_rdd.map(lambda a : (a[1], a[0]))
sorted_values = swap_business_rdd.sortByKey(False)
sorted_values_swapped = sorted_values.map(lambda x: (x[1], x[0]))

sorted_values_swapped.saveAsTextFile('gs://<your-cloud-storage-bucket>/best-business-cities')
