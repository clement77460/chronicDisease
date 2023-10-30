package fr.extract

import fr.common.spark.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

class ChronicDisease {
  def extractFile(): DataFrame = {

    val sparkUtilities = new SparkUtils()
    val spark = sparkUtilities.createSparkSession()

    // Simulate that we retrieve it from google
    val df = sparkUtilities.read_csv(spark, "resources/ingestion/chronic_disease/U.S._Chronic_Disease_Indicators__CDI_.csv")

    // Solution to store it
    // ....
    df
  }

  def cleanFile(df:DataFrame):Unit={
    df.printSchema()
    df.show(5, false)
    /*
    df.write
      .partitionBy("LocationDesc", "Topic")
      .format("parquet")
      .save("resources/optimized/chronic_disease.parquet")
    */

  }

}
