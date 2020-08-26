package com.learn.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DataSetFiltering {

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

    //Internally uses RDD
    //Takes SQL's where
    Dataset<Row> modernArtResultsWithExpression = dataSet.filter("subject = 'Modern Art' AND year >= 2007");
    modernArtResultsWithExpression.show();

    Dataset<Row> mathResultWithFilterFunction = dataSet.filter( row -> row.getAs("subject").equals("Math"));
    mathResultWithFilterFunction.show();

    //Better for programmatically building it.
    Column subject = dataSet.col("subject");
    //from functions class
    Column year = functions.col("year");

    Dataset<Row> biologyResult = dataSet.filter(subject.equalTo("Biology").and(year.geq(2007)));
    biologyResult.show();

    spark.stop();
  }

}
