package io.sharat.spark.listener

import org.apache.spark.scheduler._
import org.slf4j.LoggerFactory

/**
 * spark listner class to handle spark events
 */
class CustomSparkListener extends SparkListener {

  private var jobsCompleted: Long = 0L
  private var stagesCompleted: Long = 0L
  private var tasksCompleted : Long = 0L
  private var executorRuntime: Long = 0L
  private var recordsRead : Long = 0L
  private var recordsWritten : Long = 0L

  val log = LoggerFactory.getLogger("mySparkListener")

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    log.info("***************** Aggregate metrics *****************************")
    log.info(s" Jobs = ${jobsCompleted.toString}, Stages = ${stagesCompleted.toString}, Tasks = ${tasksCompleted.toString}")
    log.info(s" Executor runtime = ${executorRuntime.toString}ms, Records Read = ${recordsRead.toString}, Records written = ${recordsWritten.toString}")
    log.info("*****************************************************************")

  }
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    jobsCompleted += 1
  }
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stagesCompleted +=1
  }
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    tasksCompleted += 1
    executorRuntime = executorRuntime + taskEnd.taskMetrics.executorRunTime.toLong
    recordsRead = recordsRead + taskEnd.taskMetrics.inputMetrics.recordsRead.toLong
    recordsWritten = recordsWritten + taskEnd.taskMetrics.outputMetrics.recordsWritten.toLong
  }


}
