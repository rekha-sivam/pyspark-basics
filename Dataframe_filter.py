from pyspark.sql.functions import *
from pyspark.sql.types import *
def df_filtercondn(spark):
    #df_filtercondn(spark)

    df = spark.read.csv("file:///home/hadoop/Downloads/dataanalytics-main/MySQL/pet.csv")
    # df.show()
    # equals condition
    df.filter(df._c2 == "cat").show()
    # # not equals condition
    df.filter(df._c5!= "null") \
        .show(truncate=False)
    df.filter(~(df._c2 == "dog")) \
         .show(truncate=False)

def df_filtercondn_sqlexpr(spark):
    #df_filtercondn_sqlexpr(spark)

    #to filter DataFrame rows with SQL expressions
    df = spark.read.csv("file:///home/hadoop/Downloads/dataanalytics-main/MySQL/pet.csv")
    df.show()

    # Using SQL Expression
    df.filter("_c3 == 'm'").show()
    # For not equal
    df.filter("_c3 != 'm'").show()
    df.filter("_c3 <> 'm'").show()

def df_filtercondn_mulcondn(spark):
    #df_filtercondn_mulcondn(spark)

    #DataFrame based on multiple conditions
    df = spark.read.csv("file:///home/hadoop/Downloads/dataanalytics-main/MySQL/pet.csv")
    df.filter((df._c2 == "cat") & (df._c3 == "m")) \
        .show(truncate=False)

def df_filtercondn_list(spark):
    # df_filtercondn_list(spark)

    #Filter Based on List Values
    df = spark.read.csv("file:///home/hadoop/Downloads/dataanalytics-main/MySQL/pet1.csv")
    df.show()

    li = ["lu", "at", "en"]
    df.filter(df._c0.isin(li)).show()
    df.filter(~df._c1.isin(li)).show()
    df.filter(df._c1.isin(li) == False).show()
