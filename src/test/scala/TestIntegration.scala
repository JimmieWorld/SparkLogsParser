import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testtask.Utils
import org.testtask.parser.processors.{ErrorStatsAccumulator, RawDataProcessor}
import org.testtask.tasks.{Task1, Task2}

class TestIntegration extends AnyFlatSpec with Matchers {
  "Full pipeline" should "correctly parse and process a real log file" in {
    val conf = new SparkConf()
      .setAppName("TestIntegration")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    try {
      val errorStatsAcc = new ErrorStatsAccumulator()
      sc.register(errorStatsAcc)

      val filesPath = "src/test/resources/data"

      val sessionsRDD = RawDataProcessor.process(sc, filesPath, errorStatsAcc)

      sessionsRDD.count() shouldBe 100

      Task1.execute(sessionsRDD)
      Task2.execute(sessionsRDD)

      val errors = errorStatsAcc.value

      val errorArray = errors.map { case (errorType, (count, samples)) =>
        (errorType, count.toString, samples.mkString(" -|- "))
      }.toArray

      Utils.saveToCSV(
        items = errorArray,
        outputPath = "src\\main\\resources\\results\\test_errors.csv",
        header = Array("ErrorType", "Count", "Samples")
      )(_.productIterator.map(_.toString).toArray)

      errors.get("UnmatchedDocOpen") match {
        case Some((count, _)) =>
          count shouldBe 0
        case None => ()
      }

      errors.get("Warning: InvalidTimestampFormat") match {
        case Some((count, _)) =>
          count shouldBe 159
        case None => ()
      }

    } finally {
      sc.stop()
    }
  }
}