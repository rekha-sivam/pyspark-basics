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