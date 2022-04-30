EXPORT DATA OPTIONS(
  uri='gs://bda_eth/contracts/contracts-*',
  format='PARQUET',
  overwrite=true) AS
SELECT * FROM bigquery-public-data.crypto_ethereum.contracts