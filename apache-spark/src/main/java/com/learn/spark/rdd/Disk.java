package com.learn.spark.rdd;

import java.util.Arrays;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Disk {

  public static void main(String[] args) {

    //Required for windows,
    //Code runs without it as well, just throws exception at the beginning but works
    System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");

    SparkConf conf = new SparkConf()
        .setAppName("Learning Spark")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);
    //generally read file from distributed file system
    //like s3 or hdfs
    //each spark worker node then gets the part of the file
    JavaRDD<String> sentences = sc.textFile("C:\\personal-workspace\\big-tada\\apache-spark\\src\\main\\resources\\subtitles\\input.txt");

    sentences.flatMap(str -> Arrays.asList(str.split(" ")).iterator())
        .filter(word -> !NumberUtils.isNumber(word))
        .foreach(word -> System.out.println(word));

    sc.close();
  }
}
