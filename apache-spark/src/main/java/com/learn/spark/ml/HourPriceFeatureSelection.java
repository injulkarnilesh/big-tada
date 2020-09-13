package com.learn.spark.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HourPriceFeatureSelection {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("House Price Data Analysis")
        .config("spark.sql.warehouse.dir", "file///c:/tmp/")
        .getOrCreate();

    Dataset<Row> housePrices = spark.read().option("header", true)
        .option("inferSchema", true)
        .csv("big-tada/apache-spark/src/main/resources/ml/kc_house_data.csv");

    housePrices.describe().show();

    housePrices = housePrices
        .drop("id", "date", "waterfront", "view", "condition", "grade", "yr_renovated", "zipcode",
            "lat", "long");

    //Columns' correlation with label
    for (String column : housePrices.columns()) {
      double priceCorrelation = housePrices.stat().corr("price", column);
      System.out.println(String.format("Correlation Between Price and %s is %f", column, priceCorrelation));
    }

    //Get correlation between variables to identify duplicates
    for (String column : housePrices.columns()) {
      for (String column2: housePrices.columns()) {
        double corr = housePrices.stat().corr(column, column2);
        System.out.println(String.format("Correlation"
            + " Between %s and %s is %f", column, column2, corr));
      }
    }



  }

}
