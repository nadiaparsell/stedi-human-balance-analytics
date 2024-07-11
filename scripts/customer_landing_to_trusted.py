# Import libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

# Initialize job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load customer landing data
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-nadia2/customer/landing"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Only include participants that agreed to share data
PrivacyFilter_node = Filter.apply(
    frame=S3bucket_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node",
)

# Save customer trusted data
TrustedCustomerZone_node = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node,
    connection_type="s3",
    format="json",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    connection_options={
        "path": "s3://stedi-lake-house-nadia2/customer/trusted2/",
    },
    transformation_ctx="TrustedCustomerZone_node",
)

job.commit()
