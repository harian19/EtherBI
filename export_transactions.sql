EXPORT DATA OPTIONS(
  uri='gs://bda_eth/transactions/transactions-*',
  format='PARQUET',
  overwrite=true) AS
SELECT * FROM bigquery-public-data.crypto_ethereum.transactions 