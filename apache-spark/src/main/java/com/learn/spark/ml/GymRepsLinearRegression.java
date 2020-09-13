package com.learn.spark.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymRepsLinearRegression {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("Gym Data Analysis")
        .config("spark.sql.warehouse.dir", "file///c:/tmp/")
        .getOrCreate();

    Dataset<Row> gymData = spark.read().option("header", true)
        .option("inferSchema", true)
        .csv("big-tada/apache-spark/src/main/resources/ml/GymCompetition.csv");

    //Create Label and Features
    VectorAssembler vectorAssembler = new VectorAssembler();
    vectorAssembler.setInputCols(new String[] { "Age", "Height", "Weight"})
          .setOutputCol("features");
    //adds new features columns
    Dataset<Row> gymWithFeatures = vectorAssembler.transform(gymData)
        .select("features", "NoOfReps").withColumnRenamed("NoOfReps", "label");
    gymWithFeatures.show();

    LinearRegression linearRegression = new LinearRegression();
    LinearRegressionModel model = linearRegression.fit(gymWithFeatures);

    Vector coefficients = model.coefficients();
    double intercept = model.intercept();
    System.out.println(String.format("Regression Model Coeffiecnts %s and intercept %f", coefficients, intercept));

    Dataset<Row> predictedData = model.transform(gymWithFeatures);
    predictedData.show();

    spark.close();
  }

}
