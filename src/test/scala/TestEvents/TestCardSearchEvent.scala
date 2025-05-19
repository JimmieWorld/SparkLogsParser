import org.james_world.Events.CardSearchEvent
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class TestCardSearchEvent extends AnyFlatSpec with Matchers {

    "CardSearchEvent.parse" should "correctly parse valid input lines" in {
        val input = Seq(
            "CARD_SEARCH_START 23.07.2020_22:03:45",
            "$0 PBI_95949",
            "CARD_SEARCH_END",
            "5992653 PBI_95949"
        )

        val result = CardSearchEvent.parse(input)

        result shouldBe defined

        val event = result.get
        event.timestamp shouldEqual LocalDateTime.of(2020, 7, 23, 22, 3, 45)
        event.searchId shouldBe "5992653"
        event.queriesTexts should contain theSameElementsAs Seq("0 PBI_95949")
        event.relatedDocuments should contain theSameElementsAs Seq("PBI_95949")
    }

    it should "handle multiple queries correctly" in {
        val input = Seq(
            "CARD_SEARCH_START 23.07.2020_22:03:45",
            "$134 обзор вс по  закупкам",
            "$0 PBI_95949",
            "$1 PBI_95949",
            "CARD_SEARCH_END",
            "5992653 PBI_95949"
        )

        val result = CardSearchEvent.parse(input)

        result shouldBe defined
        val event = result.get
        event.searchId shouldBe "5992653"
        event.queriesTexts should contain theSameElementsAs Seq(
            "134 обзор вс по  закупкам",
            "0 PBI_95949",
            "1 PBI_95949"
        )
        event.relatedDocuments should contain theSameElementsAs Seq("PBI_95949")
    }

    it should "parse even if no queries are present between first and last line" in {
        val input = Seq(
            "CARD_SEARCH_START 23.07.2020_21:58:57",
            "CARD_SEARCH_END"
        )

        val result = CardSearchEvent.parse(input)

        result shouldBe defined
        val event = result.get
        event.queriesTexts shouldBe empty
        event.relatedDocuments shouldBe empty
    }

    it should "parse even if no related documents are present" in {
        val input = Seq(
            "CARD_SEARCH_START 23.07.2020_21:58:57",
            "$134 обособленное подразделение НДС",
            "CARD_SEARCH_END"
        )

        val result = CardSearchEvent.parse(input)

        result shouldBe defined
        val event = result.get
        event.relatedDocuments shouldBe empty
    }
}