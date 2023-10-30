package fr.transform.chronic_indicators

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{asc, desc}

class ComputeChronicIndicator(input_df:DataFrame) {
  val df:DataFrame = input_df

  def getTopicByLocation:DataFrame={
    df
      .groupBy("LocationDesc", "Topic").count()
      .orderBy(asc("LocationDesc"), desc("count"))
  }

  def getMostPresentTopic(deep: Int): Array[Row] = {
    df
      .groupBy("Topic").count()
      .orderBy(desc("count")).
      head(deep)
  }
}
