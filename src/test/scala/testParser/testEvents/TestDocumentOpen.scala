package testParser.testEvents

import org.testtask.parser.events.DocumentOpen
import org.testtask.parser.processors.{ErrorStatsAccumulator, ParsingContext}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime
import org.mockito.MockitoSugar.verifyZeroInteractions

class TestDocumentOpen extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  var errorStatsAcc: ErrorStatsAccumulator = _

  override def beforeEach(): Unit = {
    errorStatsAcc = mock(classOf[ErrorStatsAccumulator])
    super.beforeEach()
  }

  def extract(lines: List[String]): ParsingContext = {
    val bufferedIt = lines.iterator.buffered
    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    DocumentOpen.parse(context)
    context
  }

  "DocumentOpen.parse" should "correctly parse DOC_OPEN line with valid timestamp" in {
    val line = List(
      "DOC_OPEN 25.04.2025_10:00:00 -1234 DOC_123"
    )

    val context = extract(line)
    val event = context.sessionBuilder.docOpens.head

    event.searchId shouldBe "-1234"
    event.documentId shouldBe "DOC_123"
    event.timestamp shouldBe Some(LocalDateTime.of(2025, 4, 25, 10, 0, 0))
  }

  it should "use searchTimestamps if no timestamp in line" in {
    val line = List(
      "DOC_OPEN -12345 DOC123"
    )

    val context = extract(line)
    val event = context.sessionBuilder.docOpens.head

    println(errorStatsAcc.value)

    event.timestamp shouldBe None
    errorStatsAcc.value shouldBe null
  }

  it should "parse DOC_OPEN line with extra spaces or malformed spacing" in {
    val line = List(
      "DOC_OPEN   13.02.2020_21:45:55   -1723438653   RAPS013_286883"
    )

    val context = extract(line)
    val event = context.sessionBuilder.docOpens.head

    event.timestamp shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 45, 55))
    event.searchId shouldBe "-1723438653"
    event.documentId shouldBe "RAPS013_286883"

    verifyZeroInteractions(errorStatsAcc)
  }
}
