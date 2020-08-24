# Apache Spark
Distributed processing of big data.
Comes with 
* Java
* Scala
* Python

## Architecture
![Spark Arch](src/main/resources/img/SparkArch.png)

## Run
### Spark dependencies
* spark-core
* spark-sql
* hadoop-hdfs

### AWS Deployment
- AWS EMR(Elastic Map Reduce) supports building pre-configured spark cluster.
- For practice, 
    * create cluster of few EC2s
    * ssh into master node
    * Have jar of spark job copied (with code only, no spark dependencies required as spark is installed on this cluster nodes)
    * Run `spark-submit` command for the job to be run in the cluster.  

## RDD
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

Create PairRDD from RDD
- PairRDD has methods which works well in distributed environment.
```Java
JavaPairRDD<String, Long> pairRDD = rdd.mapToPair(...)
```

Coalesce Method Of RDD
- Gets RDD data on given number of partitions.
- May cause OOM as distributed data is pulled into lesser nodes.
- Used for performance reasons:
- If a big data is processed over large number of partitions, 
it would be processed faster, but after some processing like (filter)
partial result might be smaller so it would be better to reduce the number of partitions
to improve the performance.

ForEach
- Would run passed lambda on all partitions in parallel.

Collect
- Collects data to Driver's JVM.
- Be sure that result is small enough to fit into single JVM
otherwise push it to storage like HDFS.

Joins
- `JavaPairRDD` supports following joins with other `JavaPairRDD`
   * join (inner join)
   * leftOuterJoin
   * rightOuterJoins
   * fullOuterJoin
   * cartesian