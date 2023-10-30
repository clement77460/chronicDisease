package fr.scheduler

import fr.extract.ChronicDisease
import fr.transform.chronic_indicators.ComputeChronicIndicator

object Main {
  def main(args: Array[String]): Unit = {
    val chronicDisease = new ChronicDisease()
    val df = chronicDisease.extractFile()
    chronicDisease.cleanFile(df)

    val chronicModel = new ComputeChronicIndicator(input_df = df)
    chronicModel.getTopicByLocation
      .show(10, false)
    chronicModel.getMostPresentTopic(5).foreach(x=>println(x))
  }
}