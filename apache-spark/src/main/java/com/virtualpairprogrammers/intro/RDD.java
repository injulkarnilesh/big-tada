package com.virtualpairprogrammers.intro;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class RDD {

  public static void main(String[] args) {
    List<Double> numbers = Lists.newArrayList(
      1.20d, 1220.12d, 45.00d, 100.00d, 78.10, 90.45d
    );

    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkConf conf = new SparkConf()
        .setAppName("Learning Spark")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<Double> rdd = sc.parallelize(numbers);

    Double sum = rdd.reduce((n1, n2) -> n1 + n2);
    System.out.println("Sum by reduce:" + sum);

    JavaRDD<Double> sqrtRdd = rdd.map(Math::sqrt);
    sqrtRdd.collect().forEach(System.out::println);
    //collect into list then iterate

    JavaRDD<Tuple2<Double, Double>> tupleRdd = rdd.map(d -> new Tuple2<>(d, Math.sqrt(d)));
    tupleRdd.foreach(t -> System.out.println("Tuple<" + t._1 + "," +  t._2 + ">"));
    //foreach argument function is sent to all nodes of cluster
    //for that function should be Serializable
    //works fine locally, might not work on cluster

    sc.close();
  }

}
