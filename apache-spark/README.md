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

### DAG
Like Java 8 last actions trigger actual processing of Direct Acyclic Graph (Execution Plan) of tasks created by all transformations before.

While Spark is running locally (even inside editor), it starts local server at localhost:4040 where it shows the DAG of the running job.

Click on the job on Spark WebUI, to view its DAG. 

DAG is helpful for
* Knowing about jobs, stages and tasks
* Time taken for tasks
* Code(line number) which resulted in particular task
* Size of data flow in tasks

### Partitioning
Number of partitions depend on input source of RDD.

Eg. If input source is a file, it is divided into chunks of 64MB each acting as a partition.

A function executing against a partition is called a `task`.

Worker nodes have the partitions, Driver node sends functions to the worker nodes.

### Transformations
#### Narrow transformations
- when data is not moved across partitions
- Eg . mapToPair, filter

#### Wide transformation 
- when data is moved across partitions may be even across nodes resulting in overhead or serialisation and deserialisation
- Eg. `groupByKey`
Wide transformation results in shuffling.
- `reduceByKey` causes shuffling as well. 
But it does reduce of it's partitions first called `Map Side Reduce` then the partial result is shuffled reducing the load.

Try to have wide transformations towards the end so that it would have lesser data to work on.

On each shuffle (data is moved across partitions), new stage is created.
More the stages (shuffling) more the time spent in serialization-deserialization.

### Jobs
If there are multiple Actions in the plan, it results in multiple Jobs.
If two actions are executed on same RDD, whatever happened in forming that RDD happens in every job (action).

```Java
rdd = rdd.map()
    .filter()
    .groupBy();
rdd.foreach();
rdd.count();    
```
 In this example two jobs are there due to two actions.
 * foreach
 * count
 
 When foreach (action) is executed, DAG is executed. But when shuffling happens due to `groupBy` the intermittent results are pushed to the disk.
 When next action count happens, due to optimization in latest few spark versions, the execution starts from last stage (last shuffling), groupBy in our case.
 
 ### Cache/Persistence
Due to multiple Jobs, same actions might get performed multiple times (even after optimization last stage actions are executed multiple times).

By looking at the DAG, if we get to know that same operation is being performed multiple times and that operation is quite heavy,
then we can explicitly cache the results of such operations by calling `cache` on that RDD.

`cache` does cache result in the memory, which in many cases might not be sufficient.
In such cases we use `persist` method of RDD which takes different strategies of persisting/caching intermittent state like `DISK` only, `MEMORY` only, `MEMORY_DISK` which tries memory first if not possible then the disk.

Be careful to not to cache the data computation of which is not heavy because persisting cache takes some time.

DAG on WebUI shows green button to represent result was cached or read from cache.
                                          
### Skewed Keys  
Sometimes, due to uneven distribution keys, skewed keys processing load is unevenly distributed across nodes.

Which causes performance degradation.
Can be identified by looking at details of individual tasks of DAG.
To avoid skewed keys, you can salk the keys.

Eg. Append random number to keys. Might need more code to write and get back result.
Do it only if performance degradation due to skewed keys is observed.

## RDD
RDD: Resilient Distributed Dataset

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

### Joins
- `JavaPairRDD` supports following joins with other `JavaPairRDD`
   * join (inner join)
   * leftOuterJoin
   * rightOuterJoins
   * fullOuterJoin
   * cartesian

   