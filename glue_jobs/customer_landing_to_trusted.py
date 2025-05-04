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
AWSGlueDataCatalog_node1746125745105 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_landing_customer_landing", transformation_ctx="AWSGlueDataCatalog_node1746125745105")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
  CAST(email AS STRING) AS email,
  CAST(serialNumber AS STRING) AS serialNumber,
  CAST(phone AS STRING) AS phone,
  CAST(birthDay AS STRING) AS birthDay,
  CAST(customerName AS STRING) AS customerName,
  CAST(lastUpdateDate AS BIGINT) AS lastUpdateDate,
  CAST(registrationDate AS BIGINT) AS registrationDate,
  CAST(shareWithFriendsAsOfDate AS BIGINT) AS shareWithFriendsAsOfDate,
  CAST(shareWithPublicAsOfDate AS BIGINT) AS shareWithPublicAsOfDate,
  CAST(shareWithResearchAsOfDate AS BIGINT) AS shareWithResearchAsOfDate
FROM stedi_db.customer_landing
WHERE shareWithResearchAsOfDate IS NOT NULL

'''
SQLQuery_node1746294527099 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AWSGlueDataCatalog_node1746125745105}, transformation_ctx = "SQLQuery_node1746294527099")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1746294527099, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746293696204", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746294614457 = glueContext.getSink(path="s3://stedi-fai/trusted/customer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1746294614457")
AmazonS3_node1746294614457.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
AmazonS3_node1746294614457.setFormat("glueparquet", compression="snappy")
AmazonS3_node1746294614457.writeFrame(SQLQuery_node1746294527099)
job.commit()
