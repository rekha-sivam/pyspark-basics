from pyspark.sql.functions import *

def create_Dataframe(spark):
    #create_Dataframe(spark)

    data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
            ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
            ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
            ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
            ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
            ]

    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
    df = spark.createDataFrame(data=data, schema=columns)
    df.show()

def create_Dataframe_external(spark):
    #create_Dataframe_external(spark)

    #DataFrame from external data sources

    df = spark.read.csv("file:///home/hadoop/Downloads/dataanalytics-main/MySQL/pet.csv")
    df.printSchema()

def rename_colname_withfn(spark):
    #rename_colname_withfn(spark)

    #To rename DataFrame column name

    df = spark.read.csv("file:///home/hadoop/Downloads/dataanalytics-main/MySQL/pet.csv")
    df.withColumnRenamed("_c0", "PetName").printSchema()

def rename_mulcolname_withfn(spark):
    #rename_mulcolname_withfn(spark)

    #PySpark withColumnRenamed to rename multiple columns
    df = spark.read.csv("file:///home/hadoop/Downloads/dataanalytics-main/MySQL/pet.csv")

    df2 = df.withColumnRenamed("_c1","Owner name") \
    .withColumnRenamed("_c2","Pet_Type") \
    .withColumnRenamed("_c0", "PetName") \
    .withColumnRenamed("_c3", "gender") \
    .withColumnRenamed("_c4", "DOB") \
    .withColumnRenamed("_c5", "DOD")
    df2.printSchema()

def rename_mulcolname_withsql(spark):
    #rename_mulcolname_withsql(spark)
    #Using Select to rename elements
    df = spark.read.csv("file:///home/hadoop/Downloads/dataanalytics-main/MySQL/pet.csv")

    df.select(col("_c0").alias("P.name"), \
              col("_c1").alias("O.name"), \
              col("_c2").alias("P.Type"), \
              col("_c3"), col("_c4"), col("_c5")) \
        .printSchema()

def rename_mulcolname_dynamic(spark):
    #rename_mulcolname_dynamic(spark)
    #Using col() function – To Dynamically rename all or multiple columns

    df = spark.read.csv("file:///home/hadoop/Downloads/dataanalytics-main/MySQL/pet.csv")
    column = df.count()
    print(column)
    # for(i in 1:column)
    #     print(df._c[i])
        #df._c[i]=i
    #df.col()

def rename_mulcolname_todf(spark):
    #rename_mulcolname_todf(spark)
    #Using toDF() to change all columns in a PySpark DataFrame

    df = spark.read.csv("file:///home/hadoop/Downloads/dataanalytics-main/MySQL/pet.csv")
    newColumns = ["P.Name", "O.Name", "P.Type", "gender","DOB","DOD"]
    df.toDF(*newColumns).printSchema()
