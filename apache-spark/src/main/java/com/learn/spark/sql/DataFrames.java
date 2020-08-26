package com.learn.spark.sql;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;


public class DataFrames {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("Learn Spark SQL")
        .config("spark.sql.warehouse.dir", "file///c:/tmp/")
        .getOrCreate();

    Dataset<Row> dataSet = spark.read()
        .option("header", true)
        .csv("big-tada/apache-spark/src/main/resources/logs/biglog.txt");

    Dataset<Row> allLevels = dataSet.select("level");
    allLevels.show();

    Dataset<Row> logsByMonth = dataSet
        .select(col("level"),
            date_format(col("datetime"), "MMMM").as("month"),
            date_format(col("datetime"), "M").as("monthnum").cast(DataTypes.IntegerType))
        .groupBy(col("level"), col("month"), col("monthnum"))
        .count()
        .orderBy(col("monthnum"), col("level"))
        .drop(col("monthnum"));

    logsByMonth.show();

    Dataset<Row> pivot = dataSet
        .select(col("level"),
            date_format(col("datetime"), "MMMM").as("month"))
        .groupBy(col("level")).pivot("month").count().na().fill(0);
    pivot.show();

    spark.stop();
  }

}
