#!/usr/bin/env python
import sys
import csv
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

nyc =spark.read.format('csv').options(header='true',inferschema='true').load("Project/NYC_CrimeData.csv")
nyc.createOrReplaceTempView("nyc")
nyc= nyc.withColumnRenamed('Number of Records','Num_Records')
nyc.createOrReplaceTempView("nyc")

result = spark.sql("select YEAR(CMPLNT_FR_DT) as year, count(*) as total from nyc group by YEAR(CMPLNT_FR_DT)").collect()
print(result.collect())
for row in result:
  print('{0},{1}'.format(row['year'],row['total']))

for col in nyc.columns:
	query = 'select count(*) - count({0}) as Null_Count, count({1}) as Good_Count from nyc'.format(col,col)
	counts = spark.sql(query).collect()
	print('Null count for column : {0} is : '.format(col),counts)

def aggregate_columns(dataframe, dataframe_name):
	f = open('get_aggregate_results.sh','w')
	for col in nyc.columns:
		print(col)
		query = 'select `{0}` as Data_Values, count(`{0}`) as count_data from nyc group by `{0}` order by `{0}`'.format(col)
		data = spark.sql(query)
		data.select(format_string('%s,%s',data.Data_Values,data.count_data)).write.save( "Projects/" +str(col)+".txt" ,format="text")
		f.write('hfs -getmerge Projects/'+str(col)+'.txt Outputs/'+str(col)+'.txt \n')
		f.flush()
	f.close()
	subprocess.call("get_aggregate_resukts.sh")

query = 'select `KY_CD` as key_code, YEAR(CMPLNT_FR_DT) as cmplnt_year, count(`KY_CD`) as count_code from nyc GROUP BY `KY_CD`,cmplnt_year order by cmplnt_year,count_code DESC
count = spark.sql(query).collect()
print('{0},{1},{2}'.format(query.key_code,cmplnt_year,count_code))