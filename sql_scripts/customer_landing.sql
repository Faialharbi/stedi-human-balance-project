CREATE EXTERNAL TABLE IF NOT EXISTS stedi_db.customer_landing (
  customerName STRING,
  email STRING,
  phone STRING,
  birthday DATE,
  serialNumber STRING,
  registrationDate BIGINT,
  lastUpdateDate BIGINT,
  shareWithResearchAsOfDate BIGINT,
  shareWithPublicAsOfDate BIGINT,
  shareWithFriendsAsOfDate BIGINT
)
STORED AS PARQUET
LOCATION 's3://stedi-fai/landing/customer/';
