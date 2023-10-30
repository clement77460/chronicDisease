package fr.common.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class SparkUtils {
  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .appName("ChronicDisease")
      .getOrCreate()
  }
  def read_csv(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .option("header", value = true)
      .csv(filePath)
  }

}
