#Spark Code for Pre-processing and Processing. 
#Name: Saira


#add Hive conf.xml and sql jar in sprk folders

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
import os
os.environ["HADOOP_USER_NAME"] = "hdfs"
os.environ["PYTHON_VERSION"] = "3.8.5"
from pyspark.sql.functions import col

from pyspark.sql.functions import isnan, when, count
import numpy as np

sparkSession = (SparkSession
 .builder
 .appName('example-pyspark-read-and-write-from-hive')
  .config("spark.sql.warehouse.dir", '/user/hive/warehouse/')
 .enableHiveSupport()
 .getOrCreate())

#from pyspark.sql import HiveContext
#hive_context = HiveContext(sc)
#bank = hive_context.table("wqd7007.Country_WEO")
#bank.show()

# Read from Hive
df_load = sparkSession.sql('show databases')
df_load.show()

df_country_WEO = sparkSession.sql('SELECT * FROM wqd7007.Country_WEO')
df_country_WEO.show(2)
#|weocountrycode|iso|weosubjectcode|    country|   subjectdescriptor|        subjectnotes|            units|   scale|country_series_specificnotes|1980|1981|1982|1983|1984|1985|1986|1987|1988|1989|1990|1991|1992|1993|1994|1995|1996|1997|1998|1999|2000|2001|   2002|   2003|   2004|  2005|   2006|  2007|   2008|   2009|   2010|   2011|2012|2013|2014|2015|2016|2017|2018|2019|2020|2021|2022|2023|2024|2025|estimatesstartafter|



df_country_WEO.filter("Country == 'NA'").show()
df_country_WEO.where(col("Country").isNull()).show()

df_country_WEO.filter(df_country_WEO["Country"].isNotNull()).count()
df_country_WEO.filter(df_country_WEO["Country"].isNull()).count()
#df_country_WEO.filter("ISO == 'NA'").show()
#df_country_WEO.where(col("ISO").isNull()).show()
#df_country_WEO.where(col("ISO").isNotNull()).show()

#df_country_WEO.filter("1993 == 'NA'").show()
#df_country_WEO.where(col("2020").isNull()).show()
#df_country_WEO.filter(df_country_WEO.where(col("2020").isNull())).count()

#df_country_WEO.filter("2020 is NULL").count()
#df_country_WEO.filter("2020 is NOT NULL").count()
#df_country_WEO.filter(df_country_WEO["2020"].isNotNull()).count()
#df_country_WEO.filter(df_country_WEO["2020"].isNull()).count()
#df_country_WEO.select([count(when(col(c).isNull(), c)).alias(c) for c in df_country_WEO.columns]).show()

#|endofperiod|loannumber|region|countrycode|country|borrower|guarantorcountrycode|guarantor|loantype|loanstatus|interestrate|currencyofcommitment|projectid|projectname|originalprincipalamount|cancelledamount|undisbursedamount|disbursedamount|repaidtoibrd|duetoibrd|exchangeadjustment|borrowersobligation|sold3rdparty|repaid3rdparty|due3rdparty|loansheld|firstrepaymentdate|lastrepaymentdate|agreementsigningdate|boardapprovaldate|effectivedate|closeddate|lastdisbursementdate|


df_Country_Loan = sparkSession.sql('SELECT * FROM wqd7007.Country_Loan')
df_Country_Loan.show(2)

df_Country_Loan.filter("country == 'NA'").show()
df_Country_Loan.where(col("country").isNull())

#df_Country_Loan.select([count(when(isnan(c), c)).alias(c) for c in df_Country_Loan.columns]).show()

df_Country_Loan.select([count(when(col(c).isNull(), c)).alias(c) for c in df_Country_Loan.columns]).show()


df_Country_Loan.filter(df_Country_Loan["country"].isNotNull()).count()
df_Country_Loan.filter(df_Country_Loan["country"].isNull()).count()


#Filtering Data for Country Malaysia
df_Country_Loan.count()
df_Malaysia_Loan=df_Country_Loan.filter(df_Country_Loan["country"]=='Malaysia')
df_Malaysia_Loan.count()
df_country_WEO.count()
df_Malaysia_WEO=df_country_WEO.filter(df_country_WEO["Country"]=='Malaysia')
df_Malaysia_WEO.count()

#df_Malaysia_Loan.registerTempTable("df_Malaysia_Loan")
#display(sqlContext.sql("select borrower,loantype,loanstatus,disbursedamount from df_Malaysia_Loan"))


import pandas as pd
import matplotlib.pyplot as plt

# Create a Pandas series from a list of values ("[]") and plot it:

#import matplotlib.pyplot as plt
#ax = df_Malaysia_Loan[['loantype','loanstatus']].plot(kind='bar', title ="V comp", figsize=(15, 10), legend=True, fontsize=12)
#ax.set_xlabel("Hour", fontsize=12)
#ax.set_ylabel("V", fontsize=12)
#plt.show()
# subjectdescriptor
df_Malaysia_WEO.filter(df_Malaysia_WEO["subjectdescriptor"]=='Employment').show()
#Unemploymentrate
df_Malaysia_WEO.filter(df_Malaysia_WEO["subjectdescriptor"]=='Unemployment rate').show()
df_Malaysia_WEO.filter(df_Malaysia_WEO["2020"].isNull()).show()

#filter out recrds where data for last 10 years from 2000-2009 is null
df_Malaysia_WEO.filter(df_Malaysia_WEO["2014"].isNotNull()&df_Malaysia_WEO["2013"].isNotNull()&df_Malaysia_WEO["2012"].isNotNull()&df_Malaysia_WEO["2011"].isNotNull()&df_Malaysia_WEO["2010"].isNotNull()&df_Malaysia_WEO["2009"].isNull()&df_Malaysia_WEO["2008"].isNull()&df_Malaysia_WEO["2007"].isNull()&df_Malaysia_WEO["2006"].isNull()&df_Malaysia_WEO["2005"].isNull()&df_Malaysia_WEO["2004"].isNull()&df_Malaysia_WEO["2003"].isNull()&df_Malaysia_WEO["2002"].isNull()&df_Malaysia_WEO["2001"].isNull()&df_Malaysia_WEO["2000"].isNull()).count()

df_Malaysia_WEO=df_Malaysia_WEO.filter(df_Malaysia_WEO["2014"].isNotNull()&df_Malaysia_WEO["2013"].isNotNull()&df_Malaysia_WEO["2012"].isNotNull()&df_Malaysia_WEO["2011"].isNotNull()&df_Malaysia_WEO["2010"].isNotNull()&df_Malaysia_WEO["2009"].isNotNull()&df_Malaysia_WEO["2008"].isNotNull()&df_Malaysia_WEO["2007"].isNotNull()&df_Malaysia_WEO["2006"].isNotNull()&df_Malaysia_WEO["2005"].isNotNull()&df_Malaysia_WEO["2004"].isNotNull()&df_Malaysia_WEO["2003"].isNotNull()&df_Malaysia_WEO["2002"].isNotNull()&df_Malaysia_WEO["2001"].isNotNull()&df_Malaysia_WEO["2000"].isNotNull())

df_Malaysia_WEO.filter(df_Malaysia_WEO["2014"].isNotNull()&df_Malaysia_WEO["2013"].isNotNull()&df_Malaysia_WEO["2012"].isNotNull()&df_Malaysia_WEO["2011"].isNotNull()&df_Malaysia_WEO["2010"].isNotNull()&df_Malaysia_WEO["2009"].isNull()&df_Malaysia_WEO["2008"].isNull()&df_Malaysia_WEO["2007"].isNull()&df_Malaysia_WEO["2006"].isNull()&df_Malaysia_WEO["2005"].isNull()&df_Malaysia_WEO["2004"].isNull()&df_Malaysia_WEO["2003"].isNull()&df_Malaysia_WEO["2002"].isNull()&df_Malaysia_WEO["2001"].isNull()&df_Malaysia_WEO["2000"].isNull()).count()

df_Malaysia_WEO.count()
#32
df_Malaysia_WEO.select('subjectdescriptor').show(32,False)


df_Malaysia_WEO.filter(df_Malaysia_WEO["subjectdescriptor"]==('Unemployment rate' )  ).show()
df_Malaysia_WEO.filter(df_Malaysia_WEO["subjectdescriptor"]==('Population' ) ).show()

df_Malaysia_WEO.show(1)
#|weocountrycode|iso|weosubjectcode| country|   subjectdescriptor|        subjectnotes|         units|scale|country_series_specificnotes| 1980| 1981| 1982|1983| 1984|  1985| 1986| 1987| 1988|1989| 1990| 1991| 1992| 1993| 1994|1995|  1996| 1997|  1998| 1999| 2000| 2001| 2002| 2003| 2004| 2005| 2006| 2007| 2008|  2009| 2010| 2011| 2012| 2013| 2014| 2015|2016| 2017|2018| 2019|2020|2021|2022|2023|2024|2025|estimatesstartafter|

df_Malaysia_WEO.filter(df_Malaysia_WEO["subjectdescriptor"]==('Unemployment rate' )  ).select(df_Malaysia_WEO.columns[1:57]).show()

employmentrate=df_Malaysia_WEO.filter(df_Malaysia_WEO["subjectdescriptor"]==('Unemployment rate' )  ).select(df_Malaysia_WEO.columns[28:44])
employmentrate.show()
employmentrate=employmentrate.rdd.flatMap(lambda x: x).collect()

import matplotlib.pyplot as plt
#Bar Chart 
x_range=[1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014]
x =list(map(str,x_range))
y= employmentrate

plt.bar(x,y)
plt.xlabel('Year')
plt.ylabel('Unemployment %')
plt.title('Unemployment % in Malaysia from 1999-2014')
plt.show()




# Line Chart  Employmentrate
x_range=[1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014]

plt.plot(x_range,employmentrate, marker='o', markerfacecolor='blue', markersize=12, color='skyblue', linewidth=4, label="Unemployment %")
plt.legend()
plt.xlabel('Years')
plt.ylabel('Unemployment %')
plt.title('Unemployment % in Malaysia from 1999-2014')
plt.show()

# Line Chart  Population
population=df_Malaysia_WEO.filter(df_Malaysia_WEO["subjectdescriptor"]==('Population' )  ).select(df_Malaysia_WEO.columns[28:44])

population=population.rdd.flatMap(lambda x: x).collect()
x_range=[1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014]

#plt.plot(x_range, list(np.array(employmentrate)[0]), marker='o', markerfacecolor='blue', markersize=12, color='skyblue', linewidth=4, label="UnEmployment Rate")
plt.plot(x_range, population, marker='x', color='olive', linewidth=2, label="Polulation in million")
plt.legend()
plt.xlabel('Years')
plt.ylabel('Popultion in million')
plt.title('Population in 1999-2014')
plt.show()

# we would try to find a corelation between loan repayment and 
#chcking for the loanstatus
#df_Malaysia_Loan

#df_Malaysia_Loan.selectExpr("year(timestamp) AS year", "value").groupBy("year").sum()
#remove null lastrepaymentdate
df_Malaysia_Loan.filter(df_Malaysia_Loan['lastrepaymentdate'].isNull()).count()
df_Malaysia_Loan=df_Malaysia_Loan.filter(df_Malaysia_Loan['lastrepaymentdate'].isNotNull())
df_Malaysia_Loan.filter(df_Malaysia_Loan['lastrepaymentdate'].isNull()).count()


#group by year and get sum of each loan status
from pyspark.sql import functions as F
#Number of loans i each Year
df_Malaysia_Loan.select(F.date_format('lastrepaymentdate','yyyy').alias('year')).groupby('year').count().show()
df_loan_year_gro=df_Malaysia_Loan.select(F.date_format('lastrepaymentdate','yyyy').alias('year'),'disbursedamount','loanstatus').groupby('year','loanstatus').sum('disbursedamount').alias('disbursedamount').orderBy('year','loanstatus')
df_loan_year_gro.show()

#& 
#df_loan_year_gro.filter((df_loan_year_gro["year"]>=1999 ) ).show(100)
#Filter for data from 1999-2014(15 years of past payment)
df_loan_year_gro=df_loan_year_gro.filter((df_loan_year_gro["year"]>=1999 ) &(df_loan_year_gro["year"]<=2014))
#df_loan_year_gro.show()
#GRaph pf disbursed amount
df_loan_year_group=df_loan_year_gro.select('year','sum(disbursedamount)').groupby('year').sum('sum(disbursedamount)').orderBy('year')
#45678,13

#df_loan_year_group
#df_loan_year_group.show()

df_loan_year_group = df_loan_year_group.withColumnRenamed("sum(sum(disbursedamount))", "disbursedamount")
##add missing rows
columns = ['year','disbursedamount']
vals = [(2004,0),(2005,0),(2006,0),(2007,0),(2008,0),(2012,0),(2013,0)]
df = spark.createDataFrame(vals, columns)
df_loan_year_group = df_loan_year_group.union(df)
df_loan_year_group = df_loan_year_group.orderBy('year')
df_loan_year_group.show()
#mvv = df_loan_year_group.select("disbursedamount").rdd.flatMap(lambda x: x).collect()
disimbursedAmount=df_loan_year_group.select("disbursedamount").rdd.flatMap(lambda x: x).collect()

x_range=[1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014]


plt.plot(x_range,disimbursedAmount, marker='x', color='olive', linewidth=2, label="Loan Disbursed Amount")
plt.legend()
plt.xlabel('Years')
plt.ylabel('Disbursed Loan Amount in Ten Billion USD')
plt.title('Disbursed Loan Amount to Malaysia from 1999-2014')
plt.show()


#Plotting Data fro two data sets to derive a corelation
#plot unemployment rate vs the disbursed amount



# create figure and axis objects with subplots()
fig,ax = plt.subplots()
# make a plot
ax.plot(x_range,employmentrate, marker='o', markerfacecolor='blue', markersize=12, color='skyblue', linewidth=4, label="Unemployment %")
# set x-axis label
ax.set_xlabel("year",fontsize=14)
plt.legend()
# set y-axis label
ax.set_ylabel("Unemployment %",color="red",fontsize=14)

# twin object for two different y-axis on the sample plot
ax2=ax.twinx()
# make a plot with different y-axis using second axis object
ax2.plot(x_range, disimbursedAmount, marker='x', color='olive', linewidth=4, label="Loan Disbursed Amount")
ax2.set_ylabel("Loan Disbursed Amount in Ten Billion USD ",color="blue",fontsize=14)
plt.legend()
plt.title('Commparing impact of loan disbursed on unemployment % for Malaysia')

plt.show()
# save the plot as a file
fig.savefig('Commparing impact of disimnursement on un employment',
            format='jpeg',
            dpi=100,
            bbox_inches='tight')



#UNEMPLOYMENT RATE is inversely proportional to the Loan Disbursed in Malaysia. Unemployment rate starts decreasing for  loan disimburmet in near future. More loan leads to more projects being implemented and less unemployment rate.
#The same can be derived form the lona given to malaysia by World bank and unemployment data provided by IMF

##PREDICTING Unemploymentrate post 2014
#########################PREDICTING Unemploymentrate post 2014###########
##Define pandas Output data frame
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType,StructField,StringType,LongType,DoubleType,FloatType
import statsmodels.tsa.api as sm
from pyspark.sql.types import IntegerType
from pyspark.sql import Row



data_schema = StructType([
     StructField('annual_forecast_1', DoubleType(), True),
     StructField('annual_forecast_2', DoubleType(), True),
     StructField('annual_forecast_3', DoubleType(), True),
     StructField('annual_forecast_4', DoubleType(), True),
     StructField('annual_forecast_5', DoubleType(), True),
     StructField('annual_forecast_6', DoubleType(), True),
     StructField('annual_forecast_7', DoubleType(), True),
     StructField('annual_forecast_8', DoubleType(), True),
     StructField('annual_forecast_9', DoubleType(), True),
     StructField('annual_forecast_10', DoubleType(), True),
     StructField('annual_forecast_11', DoubleType(), True)
     ])
#Using User Defined Function for Prediction
@pandas_udf(data_schema, PandasUDFType.GROUPED_MAP)
def time_series_udf(data):
    #ata.set_index('Year')
    time_series_data = data['Unemployment rate']
    print(time_series_data)
    ##the model
    model_yearly = sm.ExponentialSmoothing(np.asarray(time_series_data),trend='add').fit()
    ##forecast values
    forecast_values = pd.Series(model_yearly.forecast(11),name = 'fitted_values')
    forecast_values.show()
    return pd.DataFrame({'annual_forecast_1': [forecast_values[0]], 'annual_forecast_2':[forecast_values[1]],'annual_forecast_3': [forecast_values[2]],'annual_forecast_4': [forecast_values[3]],'annual_forecast_5': [forecast_values[4]],'annual_forecast_6': [forecast_values[5]],'annual_forecast_7': [forecast_values[6]], 'annual_forecast_8':[forecast_values[7]],'annual_forecast_9': [forecast_values[8]],'annual_forecast_10':[forecast_values[9]],'annual_forecast_11': [forecast_values[10]] })

unemploymentrate=df_Malaysia_WEO.filter(df_Malaysia_WEO["subjectdescriptor"]==('Unemployment rate' )  )

unemploymentrate=unemploymentrate.drop('weocountrycode').drop('iso').drop('weosubjectcode').drop('country').drop('subjectnotes').drop('units').drop('scale').drop('country_series_specificnotes').drop('estimatesstartafter')

unemploymentrate.show()
#.select(df_Malaysia_WEO.columns[18:44])
unemploymentrate_Trans=unemploymentrate.toPandas().set_index("subjectdescriptor").transpose()
#unemploymentrate=unemploymentrate.rdd.flatMap(lambda x: x).collect()
unemploymentrate_Trans
lst=list(range(1980,2026))

for idx in range(len(lst)):
    lst[idx]='01/01/'+str(lst[idx])
#lstb=lst
#lst[:]=['01/01/'+str(x) for x in lst]
#stb

#spark.createDataFrame(lst, IntegerType()).show()
#rdd1 = sc.parallelize(lst)
#row_rdd = rdd1.map(lambda x: Row(x))
#df2=sqlContext.createDataFrame(row_rdd,['years']).show()

unemploymentrate_Trans['Year']=lst

df= sqlContext.createDataFrame(unemploymentrate_Trans)

df=spark.createDataFrame(unemploymentrate_Trans)
df=df.filter((df["Year"]>='01/01/1999' ) &(df["Year"]<='01/01/2014'))

#df.show()
#predicted_spark_df = df.groupby(['Year']).apply(time_series_udf)
df.groupby(['Year']).agg(F.min(df['Unemployment rate']).alias('Unemployment')).sort('Year').show()

predicted_spark_df =df.groupby(['Year']).apply(time_series_udf)
#predicted_spark_df =time_series_udf(df.groupby(['Year']).agg(F.min(df['Unemployment rate']).alias('Unemployment')).sort('Year'))
predicted_spark_df=spark.createDataFrame(unemploymentrate_Trans)
predicted_spark_df=predicted_spark_df.filter((predicted_spark_df["Year"]>='01/01/2015' ) &(predicted_spark_df["Year"]<='01/01/2025'))
predicted_spark_df.show()

x_range=predicted_spark_df.select("Year").rdd.flatMap(lambda x: x).collect()
y_range=predicted_spark_df.select("Unemployment rate").rdd.flatMap(lambda x: x).collect()
#plt.plot(x_range, list(np.array(employmentrate)[0]), marker='o', markerfacecolor='blue', markersize=12, color='skyblue', linewidth=4, label="UnEmployment Rate")
plt.plot(x_range,y_range, marker='x', color='olive', linewidth=2, label="")
plt.legend()
plt.xticks(rotation=90)
plt.xlabel('Year')
plt.ylabel('Predicted Unemployment %')
plt.title( 'Predicted Unemployment % in Malysia from 2015-2025')
plt.show()