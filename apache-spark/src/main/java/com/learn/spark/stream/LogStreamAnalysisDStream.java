package com.learn.spark.stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class LogStreamAnalysisDStream {

  public static void main(String[] args) throws Exception {
    System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

    SparkConf conf = new SparkConf()
        .setAppName("Streaming Logs Analysis with Spark")
        .setMaster("local[*]");

    JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));

    JavaReceiverInputDStream<String> inputDStream = sc.socketTextStream("localhost", 8787);
    JavaDStream<String> mappedData = inputDStream.map(value -> value);

    /*
    Per batch size agg
     */
//    JavaPairDStream<String, Long> aggData = mappedData
//        .mapToPair(row -> new Tuple2<>(row.split(",")[0], 1L))
//        .reduceByKey((x, y) -> x + y);

    /*
    Per batch size agg
     */
    JavaPairDStream<String, Long> windowAgg = mappedData
        .mapToPair(row -> new Tuple2<>(row.split(",")[0], 1L))
        .reduceByKeyAndWindow((x, y) -> x + y, Durations.minutes(1));

    windowAgg.print();

    sc.start();
    sc.awaitTermination();

  }

}
