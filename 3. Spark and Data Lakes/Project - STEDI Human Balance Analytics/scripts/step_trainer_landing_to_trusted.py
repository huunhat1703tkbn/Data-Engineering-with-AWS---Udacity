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
    table_name="step_trainer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1692256355551 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="AmazonS3_node1692256355551",
)

# Script generated for node Join
Join_node1692256383937 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1692256355551,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1692256383937",
)

# Script generated for node Drop Fields
DropFields_node1692256421342 = DropFields.apply(
    frame=Join_node1692256383937,
    paths=[
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "lastupdatedate",
        "registrationdate",
        "`.serialnumber`",
        "birthday",
        "phone",
        "email",
        "customername",
    ],
    transformation_ctx="DropFields_node1692256421342",
)

# Script generated for node Amazon S3
AmazonS3_node1692256484094 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1692256421342,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://stedi-data-lake/step_trainer/trusted/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1692256484094",
)

job.commit()
