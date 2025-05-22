package TestEvents

import org.james_world.ErrorStatsAccumulator
import org.james_world.Events.QuickSearchEvent
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar.verifyZeroInteractions
import org.scalatest.BeforeAndAfterEach
import java.time.LocalDateTime

class TestQuickSearchEvent extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

    var errorStatsAcc: ErrorStatsAccumulator = _

    override def beforeEach(): Unit = {
        errorStatsAcc = mock(classOf[ErrorStatsAccumulator])
        super.beforeEach()
    }

    "QuickSearchEvent.parse" should "parse a valid QS line with timestamp and result line" in {
        val lines = Seq(
            "QS Thu,_13_Feb_2020_21:38:09_+0300 {отказ в назначении экспертизы}",
            "-1723438653 RAPS001_95993 SUR_196608 SUR_192860"
        )

        val bufferedIt = lines.iterator.buffered
        val event = QuickSearchEvent.parse(bufferedIt, errorStatsAcc)

        event shouldBe defined

        val qs = event.get.asInstanceOf[QuickSearchEvent]

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

        val event = QuickSearchEvent.parse(bufferedIt, errorStatsAcc)

        event shouldBe defined
        val qs = event.get.asInstanceOf[QuickSearchEvent]

        qs.timestamp shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 38, 9))
        qs.queryText shouldBe "пенсия работающим пенсионерам"
        qs.searchId shouldBe "-981704369"
        qs.relatedDocuments should contain allOf ("PSR_70597", "PKBO_22363")

        verifyZeroInteractions(errorStatsAcc)
    }

    it should "log error if QS line is malformed" in {
        val lines = List(
            "NOT_QS invalid query text",
            "-1234 DOC123"
        )

        val bufferedIt = lines.iterator.buffered

        val event = QuickSearchEvent.parse(bufferedIt, errorStatsAcc)

        event shouldBe empty
        verify(errorStatsAcc).add((
            "QuickSearchInvalidFormat",
            "Expected QS line, got: NOT_QS invalid query text..."
        ))
    }

    it should "log error if no result line after QS" in {
        val lines = List(
            "QS 13.02.2020_21:38:09 {запрос}"
        )

        val bufferedIt = lines.iterator.buffered

        val event = QuickSearchEvent.parse(bufferedIt, errorStatsAcc)

        event shouldBe defined
        val qs = event.get.asInstanceOf[QuickSearchEvent]

        qs.searchId shouldBe ""
        qs.relatedDocuments shouldBe empty
        qs.queryText shouldBe "запрос"

        verify(errorStatsAcc).add((
            "QuickSearchMissingSearchResult",
            "[QS] Expected search result line after: QS 13.02.2020_21:38:09 {запрос}"
        ))
    }

    it should "support multiple spaces or formatting variations" in {
        val lines = List(
            "QS   13.02.2020_21:38:09   {разные пробелы}",
            "-1234 DOC123"
        )

        val bufferedIt = lines.iterator.buffered

        val event = QuickSearchEvent.parse(bufferedIt, errorStatsAcc)

        event shouldBe defined
        val qs = event.get.asInstanceOf[QuickSearchEvent]
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

        val event = QuickSearchEvent.parse(bufferedIt, errorStatsAcc)

        event shouldBe defined
        val qs = event.get.asInstanceOf[QuickSearchEvent]

        qs.queryText shouldBe "пустой результат"
        qs.searchId shouldBe "-1234"
        qs.relatedDocuments shouldBe empty

        errorStatsAcc.value shouldBe null
    }
}