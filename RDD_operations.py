def rdd_actions(spark):
    # rdd_actions(spark)

    # Create RDD from parallelize

    dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
    rdd = spark.sparkContext.parallelize(dataList)
    # The .collect() Action
    print(rdd.collect())
    # The .count() Action
    print(rdd.count())
    # The .first() Action
    print(rdd.first())
    # The .take() Action
    print(rdd.take(2))
    # The .reduce() Action
    print(rdd.reduce(lambda x, y: x + y))
    # The countByKey() Action
    marks_rdd = spark.sparkContext.parallelize(
        [('Rahul', 25), ('Swati', 26), ('Rohan', 22), ('Rahul', 23), ('Swati', 19), ('Shreya', 28), ('Abhay', 26),
         ('Rohan', 22)])
    dict_rdd = marks_rdd.countByKey().items()
    for key, value in dict_rdd:
        print(key, value)
    #The .saveAsTextFile() Action
    save_rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6])
    save_rdd.saveAsTextFile('file:///home/hadoop/file2.txt')
def rdd_transformation(spark):
    # rdd_transformation(spark)

    # The.map() Transformation
    rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
    print(rdd.map(lambda x: x + 10).collect())
    # The.filter() Transformation
    filter_rdd = spark.sparkContext.parallelize([2, 3, 4, 5, 6, 7])
    print(filter_rdd.filter(lambda x: x % 2 == 0).collect())
    # The.filter() string
    filter_rdd_2 = spark.sparkContext.parallelize(['Rahul', 'Swati', 'Rohan', 'Shreya', 'Priya'])
    print(filter_rdd_2.filter(lambda x: x.startswith('R')).collect())
    # The .union() Transformation
    union_inp = spark.sparkContext.parallelize([2, 4, 5, 6, 7, 8, 9])
    union_rdd_1 = union_inp.filter(lambda x: x % 2 == 0)
    union_rdd_2 = union_inp.filter(lambda x: x % 3 == 0)
    print(union_rdd_1.union(union_rdd_2).collect())
    #The .flatMap() Transformation
    flatmap_rdd = spark.sparkContext.parallelize(["Hey there", "This is PySpark RDD Transformations"])
    print(flatmap_rdd.flatMap(lambda x: x.split(" ")).collect())
    #PySpark Pair RDD Operations
    marks = [('Rahul', 88), ('Swati', 92), ('Shreya', 83), ('Abhay', 93), ('Rohan', 78)]
    print(spark.sparkContext.parallelize(marks).collect())
    #Transformations in Pair RDDs
    #The.reduceByKey() Transformation
    marks_rdd = spark.sparkContext.parallelize(
        [('Rahul', 25), ('Swati', 26), ('Shreya', 22), ('Abhay', 29), ('Rohan', 22), ('Rahul', 23), ('Swati', 19),
         ('Shreya', 28), ('Abhay', 26), ('Rohan', 22)])
    print(marks_rdd.reduceByKey(lambda x, y: x + y).collect())
    #The .sortByKey() Transformation
    marks_rdd = spark.sparkContext.parallelize(
        [('Rahul', 25), ('Swati', 26), ('Shreya', 22), ('Abhay', 29), ('Rohan', 22), ('Rahul', 23), ('Swati', 19),
         ('Shreya', 28), ('Abhay', 26), ('Rohan', 22)])
    print(marks_rdd.sortByKey('ascending').collect())
    # The .groupByKey() Transformation
    marks_rdd = spark.sparkContext.parallelize(
        [('Rahul', 25), ('Swati', 26), ('Shreya', 22), ('Abhay', 29), ('Rohan', 22), ('Rahul', 23), ('Swati', 19),
         ('Shreya', 28), ('Abhay', 26), ('Rohan', 22)])
    dict_rdd = marks_rdd.groupByKey().collect()
    for key, value in dict_rdd:
        print(key, list(value))
