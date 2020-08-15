# Apache Spark
Distributed processing of big data.
Comes with 
* Java
* Scala
* Python

## Starting Locally
Spark dependencies
* spark-core
* spark-sql
* hadoop-hdfs

Create SparkContext
```Java
SparkConf conf = new SparkConf()
        .setAppName("Learning Spark") //App Name
        .setMaster("local[*]"); //local master, * use available core
//SparkContext
JavaSparkContext sc = new JavaSparkContext(conf);
...
sc.close()
```
Create RDD from list
```Java
//List<Double> numbers;
JavaRDD<Double> rdd = sc.parallelize(numbers);
```