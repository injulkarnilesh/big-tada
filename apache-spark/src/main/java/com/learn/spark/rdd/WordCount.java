package com.learn.spark.rdd;

import com.learn.spark.Util;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class WordCount {

  public static void main(String[] args) {

    System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");
    SparkConf conf = new SparkConf()
        .setAppName("Learning Spark")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> lines = sc.textFile("C:\\personal-workspace\\big-tada\\apache-spark\\src\\main\\resources\\subtitles\\input.txt");
    lines.filter(line -> StringUtils.isNotEmpty(line))
        .filter(line -> !NumberUtils.isNumber(line))
        .filter(line -> !line.matches("[\\d]+:[\\d]+:[\\d]+,[\\d]+ --> [\\d]+:[\\d]+:[\\d]+,[\\d]+"))
        .flatMap(line -> Arrays.asList(line.split("\\s")).iterator())
        .map(word -> word.toLowerCase())
        .map(word -> word.replaceAll("\\W", ""))
        .filter(word -> Util.isNotBoring(word))
        .mapToPair(word -> new Tuple2<>(word, 1L))
        .reduceByKey((count1, count2) -> count1 + count2)
        .mapToPair(swap())
        .sortByKey(false)
        .mapToPair(swap())
        .take(5)
        .forEach(wordCount -> System.out.println("[" + wordCount._1 + "] -> " + wordCount._2));

    sc.close();
  }

  private static <K, V> PairFunction<Tuple2<K, V>, V, K> swap() {
    return new PairFunction<Tuple2<K, V>, V, K>() {
      @Override
      public Tuple2<V, K> call(Tuple2<K, V> tuple) throws Exception {
        return new Tuple2<>(tuple._2, tuple._1);
      }
    };
  }

}
