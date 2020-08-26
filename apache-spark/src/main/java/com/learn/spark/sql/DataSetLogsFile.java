package com.learn.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetLogsFile {

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

    dataSet.createOrReplaceTempView("logs");

    Dataset<Row> logsByMonth = spark
        .sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
            + " from logs group by level, date_format(datetime, 'MMMM') "
            + " order by cast(first(date_format(datetime, 'M')) as int)");
    logsByMonth.show(100);

    logsByMonth.createOrReplaceTempView("monthly_logs");
    Dataset<Row> totalLogs = spark.sql("select sum(total) from monthly_logs");

    totalLogs.show();

//    holds the running code so that you can open localhost:4040 for web-ui
//    Scanner sc = new Scanner(System.in);
//    sc.nextLine();

    spark.stop();
  }

}
