package testParser.testEvents.utils

import org.testTask.parser.events.utils.SearchResultParser
import org.testTask.parser.processors.{ErrorStatsAccumulator, ParsingContext}
import org.mockito.Mockito._
import org.mockito.MockitoSugar.verifyZeroInteractions
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestSearchResultParser extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  var errorStatsAcc: ErrorStatsAccumulator = _

  override def beforeEach(): Unit = {
    errorStatsAcc = mock(classOf[ErrorStatsAccumulator])
    super.beforeEach()
  }

  def extract(lines: Seq[String]): (String, Seq[String]) = {
    val bufferedIt = lines.iterator.buffered
    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    SearchResultParser.parserSearchResult(context)
  }

  "extractSearchResult" should "successfully parse a valid search result line" in {
    val lines = Seq(
      "-1723438653 RAPS001_95993 SUR_196608 SUR_192860"
    )

    val (searchId, docs) = extract(lines)

    searchId shouldBe "-1723438653"
    docs should contain allOf ("RAPS001_95993", "SUR_196608", "SUR_192860")
    verifyZeroInteractions(errorStatsAcc)
  }

  it should "parse line with only searchId and no documents" in {
    val lines = Seq(
      "-1234"
    )

    val (searchId, docs) = extract(lines)

    searchId shouldBe "-1234"
    docs shouldBe empty
    verify(errorStatsAcc).add(("Warning: SearchDocumentsMissing", "No documents found in search line: -1234"))
  }

  it should "log UnexpectedEndOfSearch on unexpected event start" in {
    val lines = List(
      "DOC_OPEN 13.02.2020_21:45:55 -1723438653 RAPS013_286883"
    )

    val result = extract(lines)

    result shouldBe ("", Nil)
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

    val (searchId, docs) = extract(lines)

    searchId shouldBe "-1234"
    docs should contain theSameElementsAs Seq("DOC1", "DOC2")
    verifyZeroInteractions(errorStatsAcc)
  }

  it should "work correctly with numbers and mixed identifiers" in {
    val lines = Seq(
      "ff12345 DOC1 DOC2 DOC3"
    )

    val (searchId, docs) = extract(lines)

    searchId shouldBe "ff12345"
    docs should contain allOf ("DOC1", "DOC2", "DOC3")
    verifyZeroInteractions(errorStatsAcc)
  }
}
