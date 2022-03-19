package data.seafin.sparkles

import org.apache.spark.SparkConf

class SparklesAppSuite extends SparklesTestSuite {
  test("runs app on test event log") {
    val sparkConf = new SparkConf()
    val userDir = System.getProperty("user.dir")

    sparkConf.set(
      Config.LOG_DIR_KEY,
      s"file://$userDir/src/test/resources/event-history-test-files"
    )
    sparkConf.set(Config.APP_ID_KEY, "application_1638868391161_1868259")
    sparkConf.set(Config.RESULT_DIR_KEY, s"file://$userDir/src/test/resources/result-test-files")
    sparkConf.set("spark.sparkles.optimizer", "RuntimeSavingOptimizer")
    val app = new SparklesApp(sparkConf)
    app.run()
  }
}
