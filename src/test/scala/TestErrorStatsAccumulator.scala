import org.testTask.parser.processors.ErrorStatsAccumulator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestErrorStatsAccumulator extends AnyFlatSpec with Matchers {

  "ErrorStatsAccumulator" should "start empty and report isZero as true" in {
    val acc = new ErrorStatsAccumulator()
    acc.isZero shouldBe true
  }

  it should "add an error entry and increase count" in {
    val acc = new ErrorStatsAccumulator()

    acc.add(("ParseFailed", "Line was invalid"))
    acc.isZero shouldBe false

    val result = acc.value
    result should contain key "ParseFailed"

    result("ParseFailed") shouldBe (1, List("Line was invalid"))
  }

  it should "collect up to 10 samples per error type" in {
    val acc = new ErrorStatsAccumulator()

    (1 to 15).foreach { i =>
      acc.add(("SampleError", s"Line $i"))
    }

    val result = acc.value
    result("SampleError") match {
      case (count, samples) =>
        count shouldBe 15
        samples.size shouldBe 10
    }
  }

  it should "merge two accumulators correctly" in {
    val acc1 = new ErrorStatsAccumulator()
    val acc2 = new ErrorStatsAccumulator()

    acc1.add(("ParseFailed", "Line 1"))
    acc1.add(("MalformedLine", "Line 2"))

    acc2.add(("ParseFailed", "Line A"))
    acc2.add(("MalformedLine", "Line B"))
    acc2.add(("MalformedLine", "Line C"))

    acc1.merge(acc2)

    val result = acc1.value

    result("ParseFailed") shouldBe (2, List("Line 1", "Line A"))
    result("MalformedLine") shouldBe (3, List("Line 2", "Line B", "Line C"))
  }

  it should "reset internal state when reset() called" in {
    val acc = new ErrorStatsAccumulator()

    acc.add(("ParseFailed", "Line 1"))
    acc.reset()

    acc.isZero shouldBe true
    acc.value shouldBe empty
  }

  it should "create a copy that does not share state" in {
    val acc = new ErrorStatsAccumulator()
    acc.add(("ParseFailed", "Line 1"))

    val copy = acc.copy()

    copy.add(("ParseFailed", "Line 2"))

    acc.value("ParseFailed") shouldBe (1, List("Line 1"))
    copy.value("ParseFailed") shouldBe (2, List("Line 1", "Line 2"))
  }

  it should "report correct value after multiple adds and merges" in {
    val master = new ErrorStatsAccumulator()

    val worker1 = new ErrorStatsAccumulator()
    val worker2 = new ErrorStatsAccumulator()

    worker1.add(("ParseFailed", "Worker1 Line 1"))
    worker1.add(("MalformedLine", "Worker1 Line 2"))

    worker2.add(("ParseFailed", "Worker2 Line 1"))
    worker2.add(("ParseFailed", "Worker2 Line 2"))

    master.merge(worker1)
    master.merge(worker2)

    val result = master.value

    result("ParseFailed") shouldBe (3, List(
      "Worker1 Line 1",
      "Worker2 Line 1",
      "Worker2 Line 2"
    ))

    result("MalformedLine") shouldBe (1, List("Worker1 Line 2"))
  }
}
