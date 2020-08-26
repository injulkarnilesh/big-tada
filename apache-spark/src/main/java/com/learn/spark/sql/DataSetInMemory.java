package com.learn.spark.sql;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataSetInMemory {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("Learn Spark SQL")
        .config("spark.sql.warehouse.dir", "file///c:/tmp/")
        .getOrCreate();

    List<Row> inMemory = new ArrayList<>();
    inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
    inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
    inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
    inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
    inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

    //useful for unit testing
    StructField[] fields = new StructField[] {
      new StructField("level", DataTypes.StringType, false, Metadata.empty()),
      new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
    };
    StructType structType = new StructType(fields);
    Dataset<Row> dataSet = spark.createDataFrame(inMemory, structType);

    dataSet.show();

    dataSet.createOrReplaceTempView("logs");
    Dataset<Row> groupedLogs = spark
        .sql("select level, count(datetime) from logs group by level order by level");
    //collect_list(datetime) non-sql agg function
    groupedLogs.show();


    Dataset<Row> logsByMonth = spark
        .sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total  from logs group by level, date_format(datetime, 'MMMM')");
    logsByMonth.show();

    spark.stop();
  }

}
