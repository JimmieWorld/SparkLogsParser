package testParser.testEvents

import org.mockito.Mockito._
import org.mockito.MockitoSugar.verifyZeroInteractions
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testtask.parser.events.SearchResult
import org.testtask.parser.processors.{ErrorStatsAccumulator, ParsingContext}


class TestSearchResult extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  var errorStatsAcc: ErrorStatsAccumulator = _

  override def beforeEach(): Unit = {
    errorStatsAcc = mock(classOf[ErrorStatsAccumulator])
    super.beforeEach()
  }

  def extract(lines: Seq[String]): SearchResult = {
    val bufferedIt = lines.iterator.buffered
    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    SearchResult.parse(context)
  }

  "extractSearchResult" should "successfully parse a valid search result line" in {
    val lines = Seq(
      "-1723438653 RAPS001_95993 SUR_196608 SUR_192860"
    )

    val event = extract(lines)

    event.searchId shouldBe "-1723438653"
    event.relatedDocuments should contain allOf ("RAPS001_95993", "SUR_196608", "SUR_192860")
    verifyZeroInteractions(errorStatsAcc)
  }

  it should "parse line with only searchId and no documents" in {
    val lines = Seq(
      "-1234"
    )

    val event = extract(lines)

    event.searchId shouldBe "-1234"
    event.relatedDocuments shouldBe empty
    verify(errorStatsAcc).add(("Warning: SearchDocumentsMissing", "No documents found in search line: -1234"))
  }

  it should "log UnexpectedEndOfSearch on unexpected event start" in {
    val lines = List(
      "DOC_OPEN 13.02.2020_21:45:55 -1723438653 RAPS013_286883"
    )

    extract(lines)

    verify(errorStatsAcc).add(
      (
        "Warning: UnexpectedEndOfSearch",
        "Expected search result line, got unexpected event start: DOC_OPEN 13.02.2020_21:45:55 -1723438653 RAPS013_286883"
      )
    )
  }

  it should "handle multiple spaces or formatting variations" in {
    val lines = Seq(
      "   -1234   DOC1   DOC2   "
    )

    val event = extract(lines)

    event.searchId shouldBe "-1234"
    event.relatedDocuments should contain theSameElementsAs Seq("DOC1", "DOC2")
    verifyZeroInteractions(errorStatsAcc)
  }

  it should "work correctly with numbers and mixed identifiers" in {
    val lines = Seq(
      "ff12345 DOC1 DOC2 DOC3"
    )

    val event = extract(lines)

    event.searchId shouldBe "ff12345"
    event.relatedDocuments should contain allOf ("DOC1", "DOC2", "DOC3")
    verifyZeroInteractions(errorStatsAcc)
  }
}
