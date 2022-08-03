# This is a sample Python script.
from doctest import master
import pandas as pd
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
#To start spark session
import pyspark

from RDD_operations import *
from Dataframe_withcolumn import *
from Dataframe_filter import *
from pyspark.sql import SparkSession

if __name__ == '__main__':
      # Get Existing SparkSession
      #To start the spark session
      spark = SparkSession.builder.master("local[1]") \
            .appName("SparkByExamples.com") \
            .getOrCreate()

      #rename_mulcolname_dynamic(spark)
      df_filtercondn_sqlexpr(spark)



