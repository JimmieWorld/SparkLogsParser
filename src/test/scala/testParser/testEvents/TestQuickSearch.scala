package testParser.testEvents

import org.testtask.parser.events.QuickSearch
import org.testtask.parser.processors.{ErrorStatsAccumulator, ParsingContext}
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar.verifyZeroInteractions
import org.scalatest.BeforeAndAfterEach

import java.time.LocalDateTime

class TestQuickSearch extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  var errorStatsAcc: ErrorStatsAccumulator = _

  override def beforeEach(): Unit = {
    errorStatsAcc = mock(classOf[ErrorStatsAccumulator])
    super.beforeEach()
  }

  def extract(lines: List[String]): ParsingContext = {
    val bufferedIt = lines.iterator.buffered
    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    QuickSearch.parse(context)
    context
  }

  "QuickSearch.parse" should "parse a valid QS line with dateTime and result line" in {
    val lines = List(
      "QS Thu,_13_Feb_2020_21:38:09_+0300 {отказ в назначении экспертизы}",
      "-1723438653 RAPS001_95993 SUR_196608 SUR_192860"
    )

    val context = extract(lines)
    val qs = context.sessionBuilder.quickSearches.head

    qs.dateTime shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 38, 9))
    qs.searchResult.searchId shouldBe "-1723438653"
    qs.queryText shouldBe "отказ в назначении экспертизы"
    qs.searchResult.relatedDocuments should contain allOf ("RAPS001_95993", "SUR_196608")

    verifyZeroInteractions(errorStatsAcc)
  }

  it should "parse QS line with simple date format" in {
    val lines = List(
      "QS 13.02.2020_21:38:09 {пенсия работающим пенсионерам}",
      "-981704369 PSR_70597 PKBO_22363"
    )

    val context = extract(lines)
    val qs = context.sessionBuilder.quickSearches.head

    qs.dateTime shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 38, 9))
    qs.queryText shouldBe "пенсия работающим пенсионерам"
    qs.searchResult.searchId shouldBe "-981704369"
    qs.searchResult.relatedDocuments should contain allOf ("PSR_70597", "PKBO_22363")

    verifyZeroInteractions(errorStatsAcc)
  }

  it should "support multiple spaces or formatting variations" in {
    val lines = List(
      "QS   13.02.2020_21:38:09   {разные пробелы}",
      "-1234 DOC123"
    )

    val context = extract(lines)
    val qs = context.sessionBuilder.quickSearches.head

    qs.dateTime shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 38, 9))
    qs.queryText shouldBe "разные пробелы"

    verifyZeroInteractions(errorStatsAcc)
  }

  it should "parse QS without related documents" in {
    val lines = List(
      "QS 13.02.2020_21:38:09 {пустой результат}",
      "-1234"
    )

    val context = extract(lines)
    val qs = context.sessionBuilder.quickSearches.head

    qs.queryText shouldBe "пустой результат"
    qs.searchResult.searchId shouldBe "-1234"
    qs.searchResult.relatedDocuments shouldBe empty

    errorStatsAcc.value shouldBe null
  }
}
