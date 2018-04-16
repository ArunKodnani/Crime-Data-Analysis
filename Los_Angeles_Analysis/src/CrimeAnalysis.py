import DataCleaning as dc
import subprocess
from pyspark.sql.functions import format_string
from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("ProjectDataCleaning") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


def aggregate_columns(dataframe, tableName):
    f = open('get_aggregate_results.sh','w')
    for col in dataframe.columns:
        query = "select "+str(col)+" as Data_Values, count("+str(col)+") as count_data from "+tableName+" group by "+str(col)+" order by "+str(col)
        data = spark.sql(query)
        data.select(format_string('%s,%s',data.Data_Values,data.count_data)).write.save( "Projects/" +str(col)+".txt" ,format="text")
        f.write('hfs -getmerge Projects/'+str(col)+'.txt /home/ak6384/Project/results/'+str(col)+'.txt \n')
        f.flush()
    f.close()
    subprocess.call("get_aggregate_results.sh")

LaCrimeDF = dc.getCrimeData()
aggregate_columns(LaCrimeDF, 'LaCrime')
groupByDate = spark.sql('select YEAR(DateOccurred) as year,count(*) as cnt from LaCrime group by YEAR(DateOccurred)').collect()


f = open('CrimeByYear.csv','w')
for row in groupByDate:
  f.write('{0},{1}\n'.format(row['year'],row['cnt']))
  f.flush()
f.close()

