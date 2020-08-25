package com.learn.spark.rdd;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Operators {

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
    JavaRDD<String> sentences = sc.parallelize(input);

    sentences.flatMap(str -> Arrays.asList(str.split(" ")).iterator())
        .filter(word -> !NumberUtils.isNumber(word))
        .foreach(word -> System.out.println(word));

    sc.close();
  }

}
