from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import to_timestamp


spark = SparkSession \
    .builder \
    .appName("ProjectDataCleaning") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


def fixColumnName(dataframe, tableName):
    desc = spark.sql("DESC " + tableName).collect()
    for element in desc:
        if ' ' in element.col_name:
            dataframe = dataframe.withColumnRenamed(element.col_name, element.col_name.replace(" ", ""))
        if '&' in element.col_name:
            dataframe = dataframe.withColumnRenamed(element.col_name, element.col_name.replace("&", ""))
        if '-' in element.col_name:
            dataframe = dataframe.withColumnRenamed(element.col_name, element.col_name.replace("-", ""))
    dataframe.createOrReplaceTempView(tableName)
    return dataframe

def generateNullValuesSummery(dataframe, tableName):
    f = open(tableName + 'NullValuesSummery.txt', 'w')
    for col in dataframe.columns:
        query = "select count(*) - count("+str(col)+") as nullCnt, count("+str(col)+") as goodcnt from " + str(tableName)
        nullCount = spark.sql(query).collect()
        f.write("Column " + str(col) + "--> Number of records with Null values: " + str(nullCount[0][0]) + "\t|\t Number of records without Null values: " + str(nullCount[0][1]) + "\n")
        f.flush()
    f.close()

def fixDateColumn(dataframe, columnName, tableName):
    dataframe = dataframe.withColumn(columnName, to_timestamp(dataframe[columnName], 'MM/dd/yyyy'))
    dataframe.createOrReplaceTempView(tableName)
    return dataframe


def loadData(filename, viewName):
    temp = spark.read.format('csv').options(header='true', inferschema='true').load(filename)
    temp.createOrReplaceTempView(viewName)
    return temp


def getCrimeData():
    # Loading csv
    LaCrimeDF = loadData('Project/data/LA.csv', 'LaCrime')
    # Fixing Column Name
    LaCrimeDF = fixColumnName(LaCrimeDF, 'LaCrime')
    # Fixing Date Columns
    LaCrimeDF = fixDateColumn(LaCrimeDF, 'DateReported', 'LaCrime')
    LaCrimeDF = fixDateColumn(LaCrimeDF, 'DateOccurred', 'LaCrime')
    generateNullValuesSummery(LaCrimeDF, 'LaCrime')
    return LaCrimeDF

def getEventData():
    # Loading csv
    LaEventDF = loadData('Project/data/events.csv', 'LaEvent')
    # Fixing Column Name
    LaEventDF = fixColumnName(LaEventDF, 'LaEvent')
    # Fixing Date Columns
    LaEventDF = fixDateColumn(LaEventDF, 'EventDateTimeStart', 'LaEvent')
    LaEventDF = fixDateColumn(LaEventDF, 'EventDateTimeEnds', 'LaEvent')
    generateNullValuesSummery(LaEventDF, 'LaEvent')
    return LaEventDF

def getBudgetData():
    # Loading csv
    LaBudgetDF = loadData('Project/data/budget.csv', 'LaBudget')
    # Fixing Column Name
    LaBudgetDF = fixColumnName(LaBudgetDF, 'LaBudget')
    generateNullValuesSummery(LaBudgetDF, 'LaBudget')
    return LaBudgetDF

def getDrinkingWaterData():
    # Loading csv
    LaDrinkingWaterDF = loadData('Project/data/drinking_water.csv', 'LaDrinkingWater')
    # Fixing Column Name
    LaDrinkingWaterDF = fixColumnName(LaDrinkingWaterDF, 'LaDrinkingWater')
    # Fixing Date Columns
    LaDrinkingWaterDF = fixDateColumn(LaDrinkingWaterDF, 'Reporting_Month', 'LaDrinkingWater')
    generateNullValuesSummery(LaDrinkingWaterDF, 'LaDrinkingWater')
    return LaDrinkingWaterDF
