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
AmazonS3_node1692254235222 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="AmazonS3_node1692254235222",
)

# Script generated for node Customer Privacy Join
CustomerPrivacyJoin_node1692254335246 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1692254235222,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyJoin_node1692254335246",
)

# Script generated for node Drop Fields
DropFields_node1692254450239 = DropFields.apply(
    frame=CustomerPrivacyJoin_node1692254335246,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1692254450239",
)

# Script generated for node Amazon S3
AmazonS3_node1692254507416 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1692254450239,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://stedi-data-lake/accelerometer/trusted/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1692254507416",
)

job.commit()
