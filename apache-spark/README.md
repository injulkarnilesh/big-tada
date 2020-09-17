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


## Spark SQL, DataFrame, DataSet
Need SparkSession

```
SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("SomeAppName")
        .config("spark.sql.warehouse.dir", "file///c:/tmp/")
        .getOrCreate();
```

Spark Session has useful, readable methods to read CSV, show data in tables.
```
Dataset<Row> dataSet = spark.read().option("header", true)
        .csv("file.csv");

dataSet.show()
```
Spark SQL supports running SQL queries on dataset.
```
dataSet.createOrReplaceTempView("students");

Dataset<Row> frenchResult = spark.sql(
    "select student_id, subject, year, score, grade from students where subject = 'French' and year >= 2007"); 
```

Spark DataFrame API (DataSet) has all the SQL features but can be build programmatically.
```
Dataset<Row> logsByMonth = dataSet
        .select(col("level"),
            date_format(col("datetime"), "MMMM").as("month"),
            date_format(col("datetime"), "M").as("monthnum").cast(DataTypes.IntegerType))
        .groupBy(col("level"), col("month"), col("monthnum"))
        .count()
        .orderBy(col("monthnum"), col("level"))
        .drop(col("monthnum"));
```
Other important features supported in Spark SQL, DataFrames
* UDF - User Defined Functions
* Partition over windows
* Joins
* Union/Intersection
* Helper methods in `org.apache.spark.sql.functions`

Spark SQL is high level API build on top of Spark RDD APIs.

Performance wise using DataFrame APIs are better than SQL. RDD and DataFrame similar performance wise.

## Spark ML
### ML
#### Supervised Learning
* Predict an outcome
* Label - Outcome, Feature - Variables
* Eg.
    * Linear Regression - Large set of outcomes
    * Logistic Regression - yes or no outcome
    * Decision Tree - Limited group of outcomes

#### UnSupervised Learning
* Find relationships
* Only has Features
* Eg.
    * Group data into groups by similarity
    * Matrix Factorization
		
#### Model
* Fit the data with features (optionally labels) to create models that can answer questions for different data.
* Model Building Process
    * Choose Algorithm
    * Select input data
    * Data Preparation - Have data features and outcomes
    * Choose model fitting parameters
    * Fit the model 
    * Evaluate model
* Model evaluation can be automated


### Spark ML 
Spark ML Library Comes in ml (dataframe based) and mllib(rdd based, to be depcrecated)

* Vector Assembler
    * Used for adding features (vector) column for variable columns
    * Label can just be rename
* Vector
	* Memory efficient than array
	* [0, 0, 1, 0] -> (4,[2], [1]) 4 size, 2 index where value is not zero, 1 non zero value 
* Traning Test Holdout data
	* Need to divide data into training data and test data.
	* Training data is used for fit, model creation
	* Test data is used for testing model transformation
	* If you are using multiple parameters and figure out the best combination
		* You divide your data into training data for building model
		* Test data for algorithm to test performance of variable config params
		* Holdout data for you to test the algorithm
		* To avoid over-fitting our data
* Model Accuracy
	* Parameters decide performance of the model
		* For Linear Regression
			* RMSE - Room Mean Square Error: Smaller the better
			* R2 (R Square): 0-1 Higher the better
		* Values Available in Regression model object's summary
		* For test data model.evaluate(testData)'s summary
* TrainValidationSplit
	* To try multiple combinations of params and evaluate best combinations
	* Input needed: 
		* Estimator: algorithm to use
		* Evaluator:  tell what value of model to optimize
		* Estimator ParamMap: combinations of params
		* trainRatio : ratio of data to use for training
* Feature Selection
	* Consider
		* Eliminate dependent variables
		* Wide variety of values for features and labels
		* Has impact of label
		* Eliminate duplicate variables
	* dataSet has describe method giving details of data
	* DataSet has .stat().corr to get correlation between variables 0- no correlation 1, -1 better correlation
	* Correlate columns with label to know which are more correlated
	* Correlate columns with other columns to 
		* Identify duplicates
		* Identify new fields that can built from existing ones
* Data Preparation
	* Acquisition
	* Cleaning
	* Feature Selection
	* Data formatting
* Non Numeric Data
	* Indexing (identify unique values)  : StringIndexer
	* Encoding : OneHotEncoding
		* Gender M,F,U -> M vector<1,0,0>, F <0,1,0>, U<0,0,1>
* Pipeline
	* All transformations like (StringIndexer, OneHotEncoder, VectorAssemblers, Algorithm) generally act as output of one stage goes as input of next
	* Spark supports building pipeline of these stages, then just run transofrm/fit on the pipeline
    * PiplelineModel has result of intermittent models, can get specific one for processing
    
### Linear Regression
>NoOfReps[label] = 3[intercept] + 0.4[coefficient] * height[feature] + 0.2 * weight + 0.5 * age
* LinearRegression Class 
	* for fit (LinearRegressionModel) and transform
	* Also can get coeffiecents and intercept

### Logistic Regression
Label is boolean 0 or 1.
Same as Linear Regression in most of the terms like  parameters
Accuracy is how many % predictions were correct.    
Result
* True Positive : Prediction correct and was 1
* False Positive : Prediction incorrect and was 1
* True Negative : Prediction correct and was 0
* False Negative : Prediction incorrect and was 0

Positive Accuracy: (True Positive)/(True Positive + False Positive)
Negative Accuracy: (True Negative)/(True Negative + False Negative)