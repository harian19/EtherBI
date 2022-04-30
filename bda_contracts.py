import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Transactions by mm-yyyy").config("spark.dynamicAllocation.enabled", True).getOrCreate()

contracts = spark.read.parquet("gs://bda_eth/contracts/*")
contracts.registerTempTable('contracts')

contracts_erc20_by_month_2021 = spark.sql("select count(*), date_trunc('month', block_timestamp) as timestamp from contracts where is_erc20 = 'true' group by date_trunc('month', block_timestamp)")
contracts_erc721_by_month_2021 = spark.sql("select count(*), date_trunc('month', block_timestamp) as timestamp from contracts where is_erc721 = 'true' group by date_trunc('month', block_timestamp)")

contracts_erc20_by_month_2021.coalesce(1).write.mode("overwrite").format('csv').option("header", "true").save('gs://bda_eth/outputs/contracts/contracts_erc20_by_month.csv')
contracts_erc721_by_month_2021.coalesce(1).write.mode("overwrite").format('csv').option("header", "true").save('gs://bda_eth/outputs/contracts/contracts_erc721_by_month.csv')
