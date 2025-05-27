package testParser.testEvents

import org.testtask.parser.events.DocumentOpen
import org.testtask.parser.processors.{ErrorStatsAccumulator, ParsingContext, SessionBuilder}
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

  "DocumentOpen.parse" should "correctly parse DOC_OPEN line with valid timestamp" in {
    val line = List(
      "DOC_OPEN 25.04.2025_10:00:00 -1234 DOC_123"
    )
    val bufferedIt = line.iterator.buffered

    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    context.searchTimestamps += ("otherId" -> LocalDateTime.now())

    val result = DocumentOpen.parse(context)

    val event = result.asInstanceOf[DocumentOpen]

    event.searchId shouldBe "-1234"
    event.documentId shouldBe "DOC_123"
    event.timestamp shouldBe Some(LocalDateTime.of(2025, 4, 25, 10, 0, 0))
  }

  it should "use searchTimestamps if no timestamp in line" in {
    val line = List(
      "DOC_OPEN -12345 DOC123"
    )
    val bufferedIt = line.iterator.buffered

    val knownTime = LocalDateTime.of(2025, 4, 25, 12, 0, 0)

    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    context.searchTimestamps += ("-12345" -> knownTime)

    val result = DocumentOpen.parse(context)

    val event = result.asInstanceOf[DocumentOpen]

    println(errorStatsAcc.value)

    event.timestamp shouldBe Some(knownTime)
    errorStatsAcc.value shouldBe null
  }

  it should "parse DOC_OPEN line with extra spaces or malformed spacing" in {
    val lines = List(
      "DOC_OPEN   13.02.2020_21:45:55   -1723438653   RAPS013_286883"
    )

    val bufferedIt = lines.iterator.buffered

    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    val result = DocumentOpen.parse(context)

    val docOpen = result.asInstanceOf[DocumentOpen]

    docOpen.timestamp.map(_.toLocalDate) shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 45, 55).toLocalDate)
    docOpen.searchId shouldBe "-1723438653"
    docOpen.documentId shouldBe "RAPS013_286883"

    verifyZeroInteractions(errorStatsAcc)
  }

  it should "not fail on multiple DOC_OPEN lines with same searchId" in {
    val lines = List(
      "DOC_OPEN 13.02.2020_21:45:55 -1723438653 RAPS013_286883",
      "DOC_OPEN 13.02.2020_21:46:00 -1723438653 SUR_196608"
    )

    val bufferedIt = lines.iterator.buffered

    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    val firstEvent = DocumentOpen.parse(context)

    val docOpen1 = firstEvent.asInstanceOf[DocumentOpen]
    docOpen1.timestamp.map(_.toLocalDate) shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 45, 55).toLocalDate)
    docOpen1.searchId shouldBe "-1723438653"
    docOpen1.documentId shouldBe "RAPS013_286883"

    val secondEvent = DocumentOpen.parse(context)

    val docOpen2 = secondEvent.asInstanceOf[DocumentOpen]
    docOpen2.timestamp.map(_.toLocalDate) shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 46, 0).toLocalDate)
    docOpen2.searchId shouldBe "-1723438653"
    docOpen2.documentId shouldBe "SUR_196608"

    verifyZeroInteractions(errorStatsAcc)
  }
}
