package com.learn.spark.ml;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionSummary;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class VideoPredictionLogisticRegression {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("House Price Data Analysis")
        .config("spark.sql.warehouse.dir", "file///c:/tmp/")
        .getOrCreate();

    Dataset<Row> chaptersViewed = spark.read().option("header", true)
        .option("inferSchema", true)
        .csv("big-tada/apache-spark/src/main/resources/ml/vppChapterViews/*.csv");

    chaptersViewed = chaptersViewed.where(col("is_cancelled").equalTo("false"))
        .drop("observation_date", "is_cancelled")
        .withColumn("firstSub", when(col("firstSub").isNull(), 0).otherwise(col("firstSub")))
        .withColumn("all_time_views",
            when(col("all_time_views").isNull(), 0).otherwise(col("all_time_views")))
        .withColumn("last_month_views",
            when(col("last_month_views").isNull(), 0).otherwise(col("last_month_views")))
        .withColumn("next_month_views",
            when(col("next_month_views").$greater(0), 0).otherwise(1))
        .withColumnRenamed("next_month_views", "label");

    chaptersViewed = new StringIndexer()
        .setInputCol("rebill_period_in_months")
        .setOutputCol("periodIndex")
        .fit(chaptersViewed).transform(chaptersViewed);

    chaptersViewed = new StringIndexer()
        .setInputCol("payment_method_type")
        .setOutputCol("paymentIndex")
        .fit(chaptersViewed).transform(chaptersViewed);

    chaptersViewed = new StringIndexer()
        .setInputCol("country")
        .setOutputCol("countryIndex")
        .fit(chaptersViewed).transform(chaptersViewed);

    OneHotEncoderEstimator oneHotEncoderEstimator = new OneHotEncoderEstimator();
    oneHotEncoderEstimator.setInputCols(new String[] {"periodIndex", "paymentIndex", "countryIndex"})
        .setOutputCols(new String[] {"periodVector", "paymentVector", "countryVector"});

    chaptersViewed = oneHotEncoderEstimator.fit(chaptersViewed).transform(chaptersViewed);

    VectorAssembler vectorAssembler = new VectorAssembler();
    chaptersViewed = vectorAssembler.setInputCols(new String[] {"firstSub", "age", "all_time_views",
        "last_month_views", "periodVector", "paymentVector", "countryVector"})
        .setOutputCol("features")
        .transform(chaptersViewed).select("features", "label");

    LogisticRegression logisticRegression = new LogisticRegression();

    Dataset<Row>[] chaptersViewedSplit = chaptersViewed.randomSplit(new double[]{0.9, 0.1});
    Dataset<Row> trainingTestData = chaptersViewedSplit[0];
    Dataset<Row> holdOutData = chaptersViewedSplit[1];

    ParamMap[] paramMaps = new ParamGridBuilder()
        .addGrid(logisticRegression.regParam(), new double[]{0.01, 0.1, 0.3, 0.5, 0.7, 1})
        .addGrid(logisticRegression.elasticNetParam(), new double[]{0, 0.5, 1})
        .build();
    TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
        .setEstimator(logisticRegression)
        .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
        .setEstimatorParamMaps(paramMaps)
        .setTrainRatio(0.9);

    TrainValidationSplitModel trainValidationSplitModel = trainValidationSplit.fit(trainingTestData);
    LogisticRegressionModel model = (LogisticRegressionModel) trainValidationSplitModel.bestModel();

    LogisticRegressionTrainingSummary summary = model.summary();
    double accuracy = summary.accuracy();
    System.out.println(String.format("Model Training accuracy : %f", accuracy));

    double bestRegParam = model.getRegParam();
    double bestElasticNetParam = model.getElasticNetParam();
    System.out.println(String.format("Best Model RegParam %f ElasticNet Param %f", bestRegParam, bestElasticNetParam));

    Vector coefficients = model.coefficients();
    double intercept = model.intercept();
    System.out.println(String.format("Regression Model Coeffiecnts %s and intercept %f", coefficients, intercept));

//    chaptersViewed.show();

    LogisticRegressionSummary regressionSummary = model.evaluate(holdOutData);
    double truePositive = regressionSummary.truePositiveRateByLabel()[1];
    double falsePositive = regressionSummary.falsePositiveRateByLabel()[1];
    double positiveAccuracy = truePositive / (truePositive + falsePositive);
    System.out.println("Holdout Positive Accuracy: " + positiveAccuracy);

    model.transform(holdOutData)
        .groupBy("label", "prediction").count()
        .show();

  }

}
