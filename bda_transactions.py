import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Transactions by mm-yyyy").config("spark.dynamicAllocation.enabled", True).getOrCreate()

#Reading table of transactions
transactions = spark.read.parquet("gs://bda_eth/transactions/*")
transactions.registerTempTable('transactions')

#Reading table of ether balances 
balances = spark.read.parquet("gs://bda_eth/balances/*")
balances.registerTempTable('balances')

#Spark sql on source tables
transactions_by_month_and_type = spark.sql("select count(*) as transactionsCount, date_trunc('month', block_timestamp) as timestamp, transaction_type from transactions where group by date_trunc('month', block_timestamp), transaction_type")
transactions_by_receive_monthly = spark.sql("select max(transactionsCount), to_address, timestamp from (select count(*) as transactionsCount, to_address, date_trunc('month', block_timestamp) as timestamp from transactions group by date_trunc('month', block_timestamp), to_address) as tmp group by tmp.timestamp, tmp.to_address")
transactions_by_send_monthly = spark.sql("select max(transactionsCount), from_address, timestamp from (select count(*) as transactionsCount, from_address, date_trunc('month', block_timestamp) as timestamp from transactions group by date_trunc('month', block_timestamp), from_address) as tmp group by tmp.timestamp, tmp.from_address")
richest_holders = spark.sql("select address, eth_balance from balances order by eth_balance desc limit 5000")

#Write to GCS as CSV
transactions_by_month_and_type.coalesce(1).write.mode("overwrite").format('csv').option("header", "true").save('gs://bda_eth/outputs/transactions/transactions_by_month_and_type')
transactions_by_receive_monthly.coalesce(1).write.mode("overwrite").format('csv').option("header", "true").save('gs://bda_eth/outputs/transactions/transactions_by_receive_monthly')
transactions_by_send_monthly.coalesce(1).write.mode("overwrite").format('csv').option("header", "true").save('gs://bda_eth/outputs/transactions/transactions_by_send_monthly')
richest_holders.coalesce(1).write.mode("overwrite").format('csv').option("header", "true").save('gs://bda_eth/outputs/balances/richest_holders')