package com.learn.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetView {

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

    //builds in memory table with given name
    dataSet.createOrReplaceTempView("students");

    Dataset<Row> frenchResult = spark.sql(
        "select student_id, subject, year, score, grade from students where subject = 'French' and year >= 2007");
    frenchResult.show();

    Dataset<Row> years = spark.sql(
        "select distinct(year) from students order by year desc");
    years.show();

    spark.stop();
  }

}
