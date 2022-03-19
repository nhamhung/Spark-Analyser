package data.seafin.sparkles.analyzer

import data.seafin.sparkles.spark.{AppData, JobData}
import data.seafin.sparkles.util.Common._

/**
  * Shows timeline of how stages were scheduled in each job.
  */
class AppTimelineAnalyzer extends Analyzer {
  private val TIMELINE_UNIT_FACTOR = 100

  override def analyze(app: AppData): Analysis = {
    analysis.addln(s"${app.appName}")
    analysis.addln(
      s"Started ${printTime(app.startTime)}, Ended ${printTime(app.endTime)} : Duration ${printDuration(app.duration)}\n"
    )
    app.jobs
      .foreach(job => {
        analysis.addln(
          s"${printTime(job.startTime)}, JOB ${job.jobId} started : Duration ${printDuration(job.duration)}"
        )
        printStageTimeline(app, job)
        analysis.addln("")
      })
    analysis.show
    analysis
  }

  /**
    * Prints stage timeline like this [ 1 ||||||||||||||| ]
    */
  def printStageTimeline(app: AppData, job: JobData): Unit = {
    val jobStartTime = job.startTime
    val jobDuration = job.duration
    val unit = if (jobDuration <= TIMELINE_UNIT_FACTOR) {
      1
    } else {
      jobDuration / TIMELINE_UNIT_FACTOR.toDouble
    }

    val stages = app.stages.filter(_.jobId == job.jobId)

    stages
      .foreach(stage => {
        val stageId = stage.stageId
        val stageStart = (stage.startTime - jobStartTime) / unit
        val stageEnd = (stage.endTime - jobStartTime) / unit
        val bar = "|" * (stageEnd.toInt - stageStart.toInt)
        analysis.addln(
          f"[${stageId}%5s ${" " * stageStart.toInt + bar + " " * (TIMELINE_UNIT_FACTOR - stageEnd.toInt).max(0)}]"
        )
      })
  }
}
