package com.learn.spark.rdd;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class PairRDD {

  public static void main(String[] args) {
    List<String> input = Lists.newArrayList(
      "WARN: Tuesday 4 September 0405",
      "ERROR: Tuesday 4 September 0408",
      "FATAL: Wednesday 5 September 1632",
      "ERROR: Friday 7 September 1854",
      "WARN: Saturday 8 September 1942"
    );

    SparkConf conf = new SparkConf()
        .setAppName("Learning Spark")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> rdd = sc.parallelize(input);

    JavaPairRDD<String, Long> pairRDD = rdd
        .mapToPair(s -> {
          String[] columns = s.split(":");
          String level = columns[0];
          return new Tuple2<>(level, 1L);
        });
    //PairRDD has methods that work well distributed

    //groupByKey groups data by key key -> [values]: PairRDD<String, Iterable<String>>
    //groupByKey is a performance hit.
    //    pairRDD.groupByKey()
    //     .foreach(tuple -> System.out.println(tuple._1 + " -> " + Iterables.size(tuple._2)));

    JavaPairRDD<String, Long> sumPairRDD = pairRDD
        .reduceByKey((value1, value2) -> value1 + value2);

    sumPairRDD.foreach(tuple -> System.out.println(tuple._1 + " -> " + tuple._2));

    sc.close();
  }

}
