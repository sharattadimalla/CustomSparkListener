package io.sharat.spark.listener


import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object TestCustomSparkListnerApp extends App {

  val logger = LoggerFactory.getLogger("mySparkApp")

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("mySparkApp")
    .getOrCreate()

  logger.info(s"Initializing ${spark.sparkContext.appName}")
  spark.sparkContext.addSparkListener(new CustomSparkListener)

  val data = spark.sparkContext.parallelize(List(1 to 10))

  data.collect()

  logger.info(s"End ${spark.sparkContext.appName}")

  spark.stop()

}
