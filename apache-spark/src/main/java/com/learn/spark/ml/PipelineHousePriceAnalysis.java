package com.learn.spark.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionSummary;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PipelineHousePriceAnalysis {

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
        .csv("big-tada/apache-spark/src/main/resources/ml/kc_house_data.csv")
        .withColumnRenamed("price", "label");

    StringIndexer conditionIndexer = new StringIndexer().setInputCol("condition")
        .setOutputCol("conditionIndex");

    StringIndexer gradIndexer = new StringIndexer().setInputCol("grade")
        .setOutputCol("gradeIndex");

    StringIndexer zipCodeIndexer = new StringIndexer().setInputCol("zipcode")
        .setOutputCol("zipcodeIndex");

    OneHotEncoderEstimator encoder = new OneHotEncoderEstimator()
        .setInputCols(new String[]{"conditionIndex", "gradeIndex", "zipcodeIndex"})
        .setOutputCols(new String[]{"conditionVector", "gradeVector", "zipcodeVector"});

    //Create Label and Features
    VectorAssembler vectorAssembler = new VectorAssembler();
    vectorAssembler.setInputCols(new String[] { "bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors", "conditionVector", "gradeVector", "zipcodeVector"})
        .setOutputCol("features");

    Dataset<Row>[] housePricesSplit = housePrices.randomSplit(new double[]{0.8, 0.2});
    Dataset<Row> trainingTestData = housePricesSplit[0];
    Dataset<Row> holdOutData = housePricesSplit[1];

    LinearRegression linearRegression = new LinearRegression();

    Pipeline pipeline = new Pipeline();
    pipeline.setStages(new PipelineStage[] {conditionIndexer, gradIndexer, zipCodeIndexer, encoder, vectorAssembler, linearRegression});
    PipelineModel pipelineModel = pipeline.fit(trainingTestData);

    LinearRegressionModel model = (LinearRegressionModel) pipelineModel.stages()[5];
    LinearRegressionTrainingSummary summary = model.summary();
    double r2 = summary.r2();
    double rmse = summary.rootMeanSquaredError();
    System.out.println(String.format("Model Training R2 : %f, RMSE %f", r2, rmse));

    Dataset<Row> predicted = pipelineModel.transform(holdOutData);
    predicted.show();
    predicted = predicted.drop("prediction");

    LinearRegressionSummary testSummary = model.evaluate(predicted);
    double testR2 = testSummary.r2();
    double testRmse = testSummary.rootMeanSquaredError();
    System.out.println(String.format("Model Test R2 : %f, RMSE %f", testR2, testRmse));

    spark.close();

  }

}
