import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node kinesis-consumer
dataframe_kinesisconsumer_node1787189998515 = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "arn:aws:kinesis:us-east-1:618509429421:stream/fiap-kinesis",
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true"
    }, 
    transformation_ctx="dataframe_kinesisconsumer_node1787189998515")

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        kinesisconsumer_node1787189998515 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")


glueContext.forEachBatch(
    frame = dataframe_kinesisconsumer_node1787189998515,
    batch_function = processBatch,
    options = {"windowSize": "100 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"})
    
job.commit()