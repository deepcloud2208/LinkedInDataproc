
from pyspark import SparkContext, SparkConf

sparkAppName = "dataproctemplate"
# sparkMasterName = "local"

def extract_values(x):
    business_id = x.split(",")[0]
    risk_category = x.split(",")[3]

    return business_id+","+risk_category

# conf = SparkConf().setMaster(sparkMasterName).setAppName(sparkAppName)
conf = SparkConf().setAppName(sparkAppName)
sc = SparkContext(conf=conf)

rdd1 = sc.textFile('gs://<your-cloud-storage-bucket>/violations_plus.csv')
rdd2 = rdd1.map(extract_values)
rdd3 = rdd2.map(lambda x: (x,1))
rdd4 = rdd3.reduceByKey(lambda x,y: (x + y))
rdd5 = rdd4.sortByKey()

rdd5.coalesce(1, shuffle = True).saveAsTextFile('gs://<your-cloud-storage-bucket>/business_risks')
