package com.learn.spark.stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

public class ViewingAnalysisStructuredStream {

  public static void main(String[] args) throws Exception {
    System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

    SparkSession session = SparkSession.builder()
        .master("local[*]")
        .appName("Course Viewing Analysis With Structured Stream")
        .getOrCreate();
    session.conf().set("spark.sql.shuffle.partitions", "12");

    Dataset<Row> dataset = session.readStream()
        .format("kafka") //file
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "viewrecords") //topic
        .load();

    dataset.createOrReplaceTempView("viewing_figures");

    String sqlAgg = "select cast(value as string) as course, sum(5) from viewing_figures group by course";
    String sqlWindow = "select window, cast(value as string) as course, sum(5) from viewing_figures group by window(timestamp, '1 minute'), course";
    Dataset<Row> rows = session.sql(sqlWindow);

    StreamingQuery query = rows.writeStream()
        .format("console") //parquet, kafka
        .outputMode(OutputMode.Complete())
        //default triggering is when new data comes in.
        //.trigger(Trigger.Continuous(Duration.create(1, TimeUnit.SECONDS)))
        //.trigger(Trigger.ProcessingTime(Duration.create(1, TimeUnit.SECONDS)))
        .start();

    query.awaitTermination();

  }

}
