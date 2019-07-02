#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# import module
from pyspark.sql import SparkSession
from pyspark.sql.types import *

#create session in order to be capable of accessing all Spark API
spark = SparkSession     .builder     .appName("Introdution to Spark DataFrame")     .config("spark.some.config.option", "some-value")     .getOrCreate()

#define data schema for file we want to read
purchaseSchema = StructType([
    StructField("Date", DateType(), True),
    StructField("Time", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Item", StringType(), True),
    StructField("Total", FloatType(), True),
    StructField("Payment", StringType(), True),
])    

# read csv file with our defined schema into Spark DataFrame, and use "tab" delimiter
purchaseDataframe = spark.read.csv(
    "dataset/purchases.csv", 
    header=True, schema=purchaseSchema, sep="\t")
#show 3 rows of our DataFrame
purchaseDataframe.show(3)


# # CHAPTER 1 PYSPARK CODING

# In[5]:


#count number of rows of our dataFrame
num_rows = purchaseDataframe.count()
print("number of rows: ", num_rows)
#show our dataFrame schema
purchaseDataframe.printSchema()
#show statistic of the data we want
purchaseDataframe.describe('Total').show()


# In[6]:


#create new dataFrame from "City" and "Total" columns
newDataframe = purchaseDataframe.select(purchaseDataframe['City'], 
            
                                        
                                        purchaseDataframe['Total'])
newDataframe.show(3); #menampilkan 3 baris DataFrame baru kita
newDataframe.printSchema() #print skema dari DataFrame baru kita


# In[7]:


#adding constant value of 10 to every row data in "Total" column
purchaseDataframe.select(purchaseDataframe['City'],
                         purchaseDataframe['Total']+10).show(3)


# In[9]:


purchaseDataframe.filter(purchaseDataframe['Total'] > 200).show(3)


# In[11]:


numTransactionEachCity = purchaseDataframe.groupBy("City").count().show(5)


# In[12]:


#import monotonically_increasing_id
from pyspark.sql.functions import monotonically_increasing_id
purchaseDataframe.show(5)
newPurchasedDataframe = purchaseDataframe.withColumn(
    "index", monotonically_increasing_id())
newPurchasedDataframe.show(7)
row2Till4 = newPurchasedDataframe.filter((newPurchasedDataframe['index']>=2) &
                                         (newPurchasedDataframe['index']<=4))
row2Till4.show()


# In[13]:


dataRow2ColumnTotal = newPurchasedDataframe.filter(newPurchasedDataframe['index']==2).select('Total')
dataRow2ColumnTotal.show()


# In[14]:


#we need to make sql temporary view for our dataFrame
purchaseDataframe.createOrReplaceTempView("purchaseSql")

#select "Total" dan "Payment" column from our sql temporary view
anotherNewDataframe = spark.sql("SELECT Total, Payment FROM purchaseSql")
anotherNewDataframe.show(3)


# In[15]:


#sorting data by "City" column alphabetically
orderByCity = spark.sql("SELECT * FROM purchaseSql ORDER BY City")
orderByCity.show(5)
#filter nilai kolom Total>50 dan urutkan berdasarkan cara pembayaran
filterAndSortWithSQL = spark.sql("SELECT * FROM purchaseSql WHERE Total>200 ORDER BY Payment")
filterAndSortWithSQL.show(4)


# In[ ]:


# Chapter 1 completed

