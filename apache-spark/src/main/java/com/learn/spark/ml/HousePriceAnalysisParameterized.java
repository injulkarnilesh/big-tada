package com.learn.spark.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionSummary;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceAnalysisParameterized {

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

    //Create Label and Features
    VectorAssembler vectorAssembler = new VectorAssembler();
    vectorAssembler.setInputCols(new String[] { "bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors", "grade"})
        .setOutputCol("features");
    //adds new features columns
    Dataset<Row> housePriceFeatures = vectorAssembler.transform(housePrices)
        .select("features", "price").withColumnRenamed("price", "label");

    Dataset<Row>[] housePricesSplit = housePriceFeatures.randomSplit(new double[]{0.8, 0.2});
    Dataset<Row> trainingTestData = housePricesSplit[0];
    Dataset<Row> holdOutData = housePricesSplit[1];

    LinearRegression linearRegression = new LinearRegression();
        //hard coded param values
//        .setMaxIter(10)
//        .setRegParam(0.3)
//        .setElasticNetParam(0.8);

    //Combinations of params
    ParamMap[] paramMaps = new ParamGridBuilder()
        .addGrid(linearRegression.regParam(), new double[]{0.01, 0.1, 0.5})
        .addGrid(linearRegression.elasticNetParam(), new double[]{0, 0.5, 1})
        //9 combinations here
        .build();

    TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
        .setEstimator(linearRegression) //algo to use
        .setEvaluator(new RegressionEvaluator().setMetricName("r2")) // what to optimize
        .setEstimatorParamMaps(paramMaps) // combinations
        .setTrainRatio(0.8);

    TrainValidationSplitModel trainValidationSplitModel = trainValidationSplit.fit(trainingTestData);
    LinearRegressionModel model = (LinearRegressionModel) trainValidationSplitModel.bestModel();

    LinearRegressionTrainingSummary summary = model.summary();
    double r2 = summary.r2();
    double rmse = summary.rootMeanSquaredError();
    System.out.println(String.format("Model Training R2 : %f, RMSE %f", r2, rmse));

    Dataset<Row> predicted = model.transform(holdOutData);
    predicted.show();

    LinearRegressionSummary testSummary = model.evaluate(holdOutData);
    double testR2 = testSummary.r2();
    double testRmse = testSummary.rootMeanSquaredError();
    System.out.println(String.format("Model Test R2 : %f, RMSE %f", testR2, testRmse));

    double bestRegParam = model.getRegParam();
    double bestElasticNetParam = model.getElasticNetParam();
    System.out.println(String.format("Best Model RegParam %f ElasticNet Param %f", bestRegParam, bestElasticNetParam));

    spark.close();

  }

}
