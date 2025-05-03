CREATE EXTERNAL TABLE IF NOT EXISTS stedi_db.step_trainer_landing (
  sensorReadingTime BIGINT,
  serialNumber STRING,
  distanceFromObject INT
)
STORED AS PARQUET
LOCATION 's3://stedi-fai/landing/step_trainer/';
