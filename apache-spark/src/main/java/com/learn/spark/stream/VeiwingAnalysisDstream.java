package com.learn.spark.stream;

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

public class VeiwingAnalysisDstream {

  public static void main(String[] args) throws Exception {
    System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

    SparkConf conf = new SparkConf()
        .setAppName("Streaming Logs Analysis with Spark")
        .setMaster("local[*]");

    JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));

    List<String> topics = Arrays.asList("viewrecords");
    Map<String, Object> params = Maps.newHashMap();
    params.put("bootstrap.servers", "localhost:9092");
    params.put("key.deserializer", StringDeserializer.class);
    params.put("value.deserializer", StringDeserializer.class);
    params.put("group.id", "consumer_local");
    params.put("auto.offset.reset", "latest");
    params.put("enable.auto.commit", false);

    ConsumerStrategy<String, String> consumerStrategy = ConsumerStrategies.Subscribe(topics, params);
    JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils
        .createDirectStream(sc,
            LocationStrategies.PreferConsistent(),
            consumerStrategy);

    JavaPairDStream<String, Long> mappedDStream = kafkaStream
        .mapToPair(item -> new Tuple2<>(item.value(), 5L));

    JavaPairDStream<Long, String> timeToCourse = mappedDStream
        .reduceByKeyAndWindow((x, y) -> x + y, Durations.minutes(1), Durations.minutes(1))
        .mapToPair(item -> item.swap())
        .transformToPair(rdd -> rdd.sortByKey(false));

    timeToCourse.print();

    sc.start();
    sc.awaitTermination();

  }

}
