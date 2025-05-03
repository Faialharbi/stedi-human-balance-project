CREATE EXTERNAL TABLE IF NOT EXISTS stedi_db.accelerometer_landing (
  user STRING,
  timestamp BIGINT,
  x FLOAT,
  y FLOAT,
  z FLOAT
)
STORED AS PARQUET
LOCATION 's3://stedi-fai/landing/accelerometer/';
