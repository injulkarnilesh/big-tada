package com.learn.spark.rdd;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

public class Join {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");
    SparkConf conf = new SparkConf()
        .setAppName("Learning Spark")
        .setMaster("local[*]");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Tuple2<Integer, Integer>> userVisits = Lists.newArrayList(
        new Tuple2(1, 10),
        new Tuple2(2, 40),
        new Tuple2(3, 20),
        new Tuple2(4, 20)
    );

    List<Tuple2<Integer, String>> usersNames = Lists.newArrayList(
      new Tuple2(1, "Nilesh"),
      new Tuple2(2, "Sagar"),
      new Tuple2(3, "Neha"),
      new Tuple2(5, "Snehal")
    );

    JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(userVisits);
    JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersNames);

    JavaPairRDD<Integer, Tuple2<String, Integer>> visitedUsers = users.join(visits);
    System.out.println("Inner Join");
    visitedUsers.foreach(user -> {
      System.out.println(String.format("UserId: %d, UserName: %s, Visits: %d", user._1, user._2._1, user._2._2));
    });

    //Mind the optional
    JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> usersWithVisits = users.leftOuterJoin(visits);
    System.out.println("Left Inner Join");
    usersWithVisits.foreach(user -> {
      System.out.println(String.format("UserId: %d, UserName: %s, Visits: %d", user._1, user._2._1, user._2._2.orElse(0)));
    });

    /*
    join
    leftOuterJoin
    rightOuterJoin
    fullOuterJoin
    cartesian
     */
  }

}
