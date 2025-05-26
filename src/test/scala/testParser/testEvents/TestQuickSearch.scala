package testParser.testEvents

import org.testTask.parser.events.QuickSearch
import org.testTask.parser.processors.{ErrorStatsAccumulator, ParsingContext}
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

  "QuickSearch.parse" should "parse a valid QS line with timestamp and result line" in {
    val lines = Seq(
      "QS Thu,_13_Feb_2020_21:38:09_+0300 {отказ в назначении экспертизы}",
      "-1723438653 RAPS001_95993 SUR_196608 SUR_192860"
    )

    val bufferedIt = lines.iterator.buffered
    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    val event = QuickSearch.parse(context)

    val qs = event.asInstanceOf[QuickSearch]

    qs.timestamp shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 38, 9))
    qs.searchId shouldBe "-1723438653"
    qs.queryText shouldBe "отказ в назначении экспертизы"
    qs.relatedDocuments should contain allOf ("RAPS001_95993", "SUR_196608")

    verifyZeroInteractions(errorStatsAcc)
  }

  it should "parse QS line with simple date format" in {
    val lines = List(
      "QS 13.02.2020_21:38:09 {пенсия работающим пенсионерам}",
      "-981704369 PSR_70597 PKBO_22363"
    )

    val bufferedIt = lines.iterator.buffered

    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    val event = QuickSearch.parse(context)

    val qs = event.asInstanceOf[QuickSearch]

    qs.timestamp shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 38, 9))
    qs.queryText shouldBe "пенсия работающим пенсионерам"
    qs.searchId shouldBe "-981704369"
    qs.relatedDocuments should contain allOf ("PSR_70597", "PKBO_22363")

    verifyZeroInteractions(errorStatsAcc)
  }

  it should "support multiple spaces or formatting variations" in {
    val lines = List(
      "QS   13.02.2020_21:38:09   {разные пробелы}",
      "-1234 DOC123"
    )

    val bufferedIt = lines.iterator.buffered

    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    val event = QuickSearch.parse(context)

    val qs = event.asInstanceOf[QuickSearch]
    qs.timestamp shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 38, 9))
    qs.queryText shouldBe "разные пробелы"

    verifyZeroInteractions(errorStatsAcc)
  }

  it should "parse QS without related documents" in {
    val lines = List(
      "QS 13.02.2020_21:38:09 {пустой результат}",
      "-1234"
    )

    val bufferedIt = lines.iterator.buffered

    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    val event = QuickSearch.parse(context)

    val qs = event.asInstanceOf[QuickSearch]

    qs.queryText shouldBe "пустой результат"
    qs.searchId shouldBe "-1234"
    qs.relatedDocuments shouldBe empty

    errorStatsAcc.value shouldBe null
  }
}
