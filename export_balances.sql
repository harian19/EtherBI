EXPORT DATA OPTIONS(
  uri='gs://bda_eth/balances/*',
  format='PARQUET',
  overwrite=true) AS
SELECT * FROM bigquery-public-data.crypto_ethereum.balances 