package fr.extract
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("ChronicDisease")
      .getOrCreate()

    val df = spark.read
      .option("header", value = true)
      .csv("resources/U.S._Chronic_Disease_Indicators__CDI_.csv")

    df.printSchema()
    spark.stop()
  }
}