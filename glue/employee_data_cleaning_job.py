import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame


# --------------------------------------------------
# Initialize AWS Glue Job
# --------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args["JOB_NAME"], args)


# --------------------------------------------------
# S3 Configuration
# --------------------------------------------------

INPUT_PATH = "s3://hitesh-event-driven-etl/older-data/"

OUTPUT_PATH = "s3://hitesh-event-driven-etl/new-data2/"


# --------------------------------------------------
# Data Quality Rules
# --------------------------------------------------

DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount > 0
]
"""


# --------------------------------------------------
# Read CSV Files From Amazon S3
# --------------------------------------------------

input_dynamic_frame = glueContext.create_dynamic_frame.from_options(

    connection_type="s3",

    connection_options={
        "paths": [INPUT_PATH],
        "recurse": True
    },

    format="csv",

    format_options={
        "quoteChar": "\"",
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False
    },

    transformation_ctx="input_dynamic_frame"
)


# --------------------------------------------------
# Convert DynamicFrame To Spark DataFrame
# --------------------------------------------------

input_dataframe = input_dynamic_frame.toDF()


# --------------------------------------------------
# Remove Duplicate Records
# --------------------------------------------------

cleaned_dataframe = input_dataframe.dropDuplicates()


# --------------------------------------------------
# Force Single Output Partition
# --------------------------------------------------

single_file_dataframe = cleaned_dataframe.coalesce(1)


# --------------------------------------------------
# Convert Spark DataFrame Back To DynamicFrame
# --------------------------------------------------

output_dynamic_frame = DynamicFrame.fromDF(

    single_file_dataframe,

    glueContext,

    "output_dynamic_frame"
)


# --------------------------------------------------
# Perform Data Quality Check
# --------------------------------------------------

EvaluateDataQuality().process_rows(

    frame=output_dynamic_frame,

    ruleset=DEFAULT_DATA_QUALITY_RULESET,

    publishing_options={
        "dataQualityEvaluationContext": "ETLDataQuality",
        "enableDataQualityResultsPublishing": True
    },

    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)


# --------------------------------------------------
# Write Cleaned Data To Amazon S3
# --------------------------------------------------

glueContext.write_dynamic_frame.from_options(

    frame=output_dynamic_frame,

    connection_type="s3",

    format="csv",

    connection_options={
        "path": OUTPUT_PATH,
        "partitionKeys": []
    },

    format_options={
        "writeHeader": True
    },

    transformation_ctx="output_s3"
)


# --------------------------------------------------
# Complete AWS Glue Job
# --------------------------------------------------

job.commit()