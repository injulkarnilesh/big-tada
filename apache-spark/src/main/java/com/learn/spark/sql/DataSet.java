package com.learn.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSet {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("Learn Spark SQL")
        .config("spark.sql.warehouse.dir", "file///c:/tmp/")
        .getOrCreate();

    Dataset<Row> dataSet = spark.read().option("header", true)
        .csv("big-tada/apache-spark/src/main/resources/exams/students.csv");

    dataSet.show();

    long count = dataSet.count();
    System.out.println(String.format("Students CSV has %d records", count));

    Row firstRow = dataSet.first();
    String subject = (String) firstRow.get(2);
    System.out.println("First Subject: " + subject);

    //CSV all internal types are string
    Integer firstYear = Integer.parseInt(firstRow.getAs("year"));
    System.out.println("First Year: " + firstYear);

    spark.stop();
  }

}
