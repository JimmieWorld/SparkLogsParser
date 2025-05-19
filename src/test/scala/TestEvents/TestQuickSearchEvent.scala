import org.james_world.Events.QuickSearchEvent
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class TestQuickSearchEvent extends AnyFlatSpec with Matchers {

    "QuickSearchEvent.parse" should "correctly parse valid input with timestamp, query and documents" in {
        val lines = Seq(
            "QS Tue,_11_Aug_2020_12:05:06_+0300 {198н}",
            "239407215 LAW_284744 LAW_213991 LAW_356590 LAW_323781"
        )

        val result = QuickSearchEvent.parse(lines)

        result shouldBe defined
        val event = result.get
        event.timestamp shouldEqual LocalDateTime.of(2020, 8, 11, 12, 5, 6)
        event.searchId shouldBe "239407215"
        event.queryText shouldBe "198н"
        event.relatedDocuments should contain theSameElementsAs Seq("LAW_284744", "LAW_213991", "LAW_356590", "LAW_323781")
    }

    it should "parse query text correctly when surrounded by braces" in {
        val lines = Seq(
            "QS SomeRandomDate {test query here}",
            "abc123 DOC_1 DOC_2"
        )

        val result = QuickSearchEvent.parse(lines)
        val event = result.get

        event.queryText shouldEqual "test query here"
    }


    it should "extract searchId from last line" in {
        val lines = Seq(
            "QS dummy line",
            "-123 DOC_1 DOC_2 DOC_3"
        )

        val result = QuickSearchEvent.parse(lines)
        val event = result.get

        event.searchId shouldEqual "-123"
    }

    it should "extract multiple related documents" in {
        val lines = Seq(
            "QS dummy date {}",
            "abc123 DOC_1 DOC_2 DOC_3 DOC_4"
        )

        val result = QuickSearchEvent.parse(lines)
        val event = result.get

        event.relatedDocuments shouldEqual Seq("DOC_1", "DOC_2", "DOC_3", "DOC_4")
    }

    it should "handle empty document list gracefully" in {
        val lines = Seq(
            "QS 31.07.2020_17:11:47 {Some long query}",
            "123456"
        )

        val result = QuickSearchEvent.parse(lines)
        val event = result.get

        event.relatedDocuments shouldEqual Seq.empty
    }

    it should "parse complex queries inside braces" in {
        val lines = Seq(
            "QS 2023-01-01T12:00:00Z {Некоммерческой организации «Региональный фонд капитального ремонта многоквартирных домов» омская область}",
            "search987 DOC_X DOC_Y DOC_Z"
        )

        val result = QuickSearchEvent.parse(lines)
        val event = result.get

        event.queryText shouldEqual "Некоммерческой организации «Региональный фонд капитального ремонта многоквартирных домов» омская область"
    }

    it should "ignore extra spaces around query in braces" in {
        val lines = Seq(
            "QS 11.08.2020_12:05:06 {   spaced query text   }",
            "search123 DOC_1 DOC_2"
        )

        val result = QuickSearchEvent.parse(lines)
        val event = result.get

        event.queryText shouldEqual "spaced query text"
    }
}