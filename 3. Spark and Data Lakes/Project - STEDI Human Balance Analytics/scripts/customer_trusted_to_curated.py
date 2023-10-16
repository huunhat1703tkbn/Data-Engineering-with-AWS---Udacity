import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1692255192316 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="AmazonS3_node1692255192316",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1692255272950 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1692255192316,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node1692255272950",
)

# Script generated for node Drop Fields
DropFields_node1692255517595 = DropFields.apply(
    frame=CustomerPrivacyFilter_node1692255272950,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1692255517595",
)

# Script generated for node Amazon S3
AmazonS3_node1692255586078 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1692255517595,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-data-lake/customer/curated/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1692255586078",
)

job.commit()
