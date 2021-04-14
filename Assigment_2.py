#!/usr/bin/env python
# coding: utf-8

# In[89]:


df=spark.read.csv('/Users/condenast/Downloads/json/ct_rr.csv',header=True)


# In[90]:


df=df.dropDuplicates()


# In[91]:


df=df.withColumn('year',F.year(F.col('ts'))).withColumn('month',F.month(F.col('ts'))).withColumn('week',F.weekofyear(F.col('ts'))).withColumn('day',F.dayofmonth(F.col('ts'))).withColumn('hour',F.hour(F.col('ts')))


# In[81]:


from pyspark.sql import functions as F 
from pyspark.sql.window import Window


# In[82]:


df.printSchema()


# In[92]:


df=df.withColumn("week_date", F.to_date(F.concat(F.col("week"),F.lit("/"),F.col("year")), "w/yyyy"))
df=df.fillna({'week_date':'2018-12-31'})


# In[97]:


df=df.withColumn('cohort_week_date',F.min('week_date').over(w))


# In[98]:


w=Window.partitionBy('number')


# In[106]:


result=df.groupby('cohort_week_date').pivot('week_date').agg(F.countDistinct('number')).orderBy('cohort_week_date')


# In[107]:


result.show()


# In[ ]:




