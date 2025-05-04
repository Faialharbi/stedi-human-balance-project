import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746204114995 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1746204114995")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746204129933 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted_filtered", transformation_ctx="AWSGlueDataCatalog_node1746204129933")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT a.*
FROM stedi_db.accelerometer_trusted_filtered a
JOIN (
  SELECT DISTINCT sensorreadingtime
  FROM stedi_db.step_trainer_trusted
) s
ON a.timestamp = s.sensorreadingtime
JOIN stedi_db.customer_trusted c
ON a.user = c.email
WHERE c.shareWithResearchAsOfDate IS NOT NULL
'''
SQLQuery_node1746204164635 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":AWSGlueDataCatalog_node1746204129933, "c":AWSGlueDataCatalog_node1746204114995}, transformation_ctx = "SQLQuery_node1746204164635")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1746204164635, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746204111365", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746204463413 = glueContext.getSink(path="s3://stedi-fai/curated/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1746204463413")
AmazonS3_node1746204463413.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
AmazonS3_node1746204463413.setFormat("glueparquet", compression="snappy")
AmazonS3_node1746204463413.writeFrame(SQLQuery_node1746204164635)
job.commit()
