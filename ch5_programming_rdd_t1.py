from project_lib import Project
from pyspark.sql import SparkSession
import re

# Cloud Object Storage
IBM_COS = 'cos://big-data-education.iba-gomel-2/education/spark/data'
ConnectionName = 'IBM COS - s3.eu-de.cloud-object-storage.appdomain.cloud'

# Get connection credentials
project = Project.access()
credentials = project.get_connection(name=ConnectionName)

# Runtime connection to the cos
prefix = "fs.cos.iba-gomel-2"
hconf = sc._jsc.hadoopConfiguration()
hconf.set('fs.cos.impl', 'com.ibm.stocator.fs.ObjectStoreFileSystem')
hconf.set('fs.stocator.scheme.list', 'cos')
hconf.set('fs.stocator.cos.impl', 'com.ibm.stocator.fs.cos.COSAPIClient')
hconf.set('fs.stocator.cos.scheme', 'cos')
hconf.set(f'{prefix}.key', credentials['access_key'])
hconf.set(f'{prefix}.secret.key', credentials['secret_key'])
hconf.set(f'{prefix}.endpoint', credentials['url'])
hconf.set(f'{prefix}.iam.api.key', credentials['api_key'])

# Create SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()

# Create SparkContext
sc = spark.sparkContext

# Get number of words
sc.textFile(f'{IBM_COS}/README.md').flatMap(lambda line: re.sub('\W+', ' ', line).split()).count()