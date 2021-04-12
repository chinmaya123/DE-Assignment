#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import functions as F 

# In[2]:


spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


# In[3]:


df=spark.read.csv('/Users/condenast/Downloads/json/ct_rr.csv',header=True)


# In[4]:


df=df.dropDuplicates()


# In[5]:


df=df.withColumn('year',F.year(F.col('ts'))).withColumn('month',F.month(F.col('ts'))).withColumn('week',F.date_format(F.to_date(F.col('ts')), "W")).withColumn('day',F.dayofmonth(F.col('ts'))).withColumn('hour',F.hour(F.col('ts')))


# In[6]:


df_post_rollup=df.rollup(df["number"],df["year"],df["month"],df["week"],df['day'],df["hour"]).count().sort("number","year","month","week","day","hour").select("number","year","month","week","day","hour","count")


# In[7]:


#Calculating the Monthly Avg, Daily Avg,Weekly Avg and Hourly Avg for each user
result=df_post_rollup.groupBy('number').agg(F.avg(F.when((df_post_rollup['month'].isNotNull()) & (df_post_rollup['week'].isNull()) & (df_post_rollup['day'].isNull()) & (df_post_rollup['hour'].isNull()), df_post_rollup['count'])).alias('Monthly Avg'),F.avg(F.when((df_post_rollup['day'].isNotNull()) & (df_post_rollup['hour'].isNull()), df_post_rollup['count'])).alias('Daily Avg'), F.avg(F.when((df_post_rollup['week'].isNotNull()) & (df_post_rollup['day'].isNull()) & (df_post_rollup['hour'].isNull()), df_post_rollup['count'])).alias('Weekly Avg'),F.avg(F.when(df_post_rollup['hour'].isNotNull(), df_post_rollup['count'])).alias('Hourly Avg'))


# In[8]:


result.show()


# In[ ]:




