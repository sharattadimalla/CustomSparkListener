
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession

object TestCustomSparkListnerApp extends App {

  val logger = LoggerFactory.getLogger(this.getClass)

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Spark Custom Listner")
    .getOrCreate()

  logger.info("Begin TestCustomSparkListenerApp")

  spark.sparkContext.addSparkListener(new CustomSparkListener)

  val data = spark.sparkContext.parallelize(List(1 to 10))

  data.collect()

  logger.info("End TestCustomSparkListenerApp")

  spark.stop()

}
