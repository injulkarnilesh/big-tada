package com.virtualpairprogrammers.intro;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

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

    sc.close();
  }

}
