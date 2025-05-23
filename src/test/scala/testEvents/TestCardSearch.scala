package testEvents

import org.james_world.{ErrorStatsAccumulator, ParsingContext}
import org.james_world.events.CardSearch
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.MockitoSugar.verifyZeroInteractions
import org.scalatest.BeforeAndAfterEach

import java.time.LocalDateTime

class TestCardSearch extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  var errorStatsAcc: ErrorStatsAccumulator = _

  override def beforeEach(): Unit = {
    errorStatsAcc = mock(classOf[ErrorStatsAccumulator])
    super.beforeEach()
  }

  "CardSearch.parse" should "parse a simple card search with one query line and result" in {
    val lines = List(
      "CARD_SEARCH_START 13.02.2020_21:59:25",
      "$134 основные средства федеральный стандарт",
      "CARD_SEARCH_END",
      "11104369 PKBO_31048 LAW_283870"
    )

    val bufferedIt = lines.iterator.buffered
    val context = ParsingContext(bufferedIt, errorStatsAcc)
    val event = CardSearch.parse(context)

    val cardSearch = event.asInstanceOf[CardSearch]

    cardSearch.timestamp shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 59, 25))
    cardSearch.queriesTexts should contain theSameElementsAs Seq("134 основные средства федеральный стандарт")
    cardSearch.searchId shouldEqual "11104369"
    cardSearch.relatedDocuments should contain allOf ("PKBO_31048", "LAW_283870")

    verifyZeroInteractions(errorStatsAcc)
  }

  it should "parse card search with multiple query lines" in {
    val lines = List(
      "CARD_SEARCH_START Thu,_13_Feb_2020_21:59:25_+0300",
      "$134 основные средства федеральный стандарт",
      "$135 налог на имущество",
      "CARD_SEARCH_END",
      "11104369 PBI_238073 LAW_344754"
    )

    val bufferedIt = lines.iterator.buffered
    val context = ParsingContext(bufferedIt, errorStatsAcc)
    val event = CardSearch.parse(context)

    val cardSearch = event.asInstanceOf[CardSearch]

    cardSearch.queriesTexts should contain theSameElementsAs Seq(
      "134 основные средства федеральный стандарт",
      "135 налог на имущество"
    )
    cardSearch.searchId shouldEqual "11104369"
    cardSearch.relatedDocuments should contain allOf ("PBI_238073", "LAW_344754")

    verifyZeroInteractions(errorStatsAcc)
  }

  it should "fail if CARD_SEARCH_END is missing" in {
    val lines = List(
      "CARD_SEARCH_START 13.02.2020_21:59:25",
      "$134 основные средства федеральный стандарт"
    )

    val bufferedIt = lines.iterator.buffered
    val context = ParsingContext(bufferedIt, errorStatsAcc)
    val event = CardSearch.parse(context)

    val cardSearch = event.asInstanceOf[CardSearch]
    cardSearch.queriesTexts should contain theSameElementsAs Seq(
      "134 основные средства федеральный стандарт"
    )

    verify(errorStatsAcc).add(("CardSearchMalformedEvent", "Missing CARD_SEARCH_END"))
  }

  it should "fail on unexpected line instead of CARD_SEARCH_END" in {
    val lines = List(
      "CARD_SEARCH_START 13.02.2020_21:59:25",
      "$134 основные средства федеральный стандарт",
      "SESSION_END 13.02.2020_22:07:57"
    )

    val bufferedIt = lines.iterator.buffered
    val context = ParsingContext(bufferedIt, errorStatsAcc)
    val event = CardSearch.parse(context)

    val cardSearch = event.asInstanceOf[CardSearch]
    cardSearch.queriesTexts should contain theSameElementsAs Seq(
      "134 основные средства федеральный стандарт"
    )

    verify(errorStatsAcc).add(
      (
        "CardSearchUnexpectedLine",
        "Expected CARD_SEARCH_END, got: SESSION_END 13.02.2020_22:07:57"
      )
    )
  }

  it should "parse without result line after CARD_SEARCH_END" in {
    val lines = List(
      "CARD_SEARCH_START 13.02.2020_21:59:25",
      "$134 основные средства федеральный стандарт",
      "CARD_SEARCH_END"
    )

    val bufferedIt = lines.iterator.buffered
    val context = ParsingContext(bufferedIt, errorStatsAcc)
    val event = CardSearch.parse(context)

    val cardSearch = event.asInstanceOf[CardSearch]

    cardSearch.queriesTexts should contain theSameElementsAs Seq("134 основные средства федеральный стандарт")
    cardSearch.searchId shouldEqual ""
    cardSearch.relatedDocuments shouldEqual Seq.empty

    verify(errorStatsAcc).add(
      (
        "CardSearchMissingSearchResult",
        "[CARD_SEARCH_START] Expected search result line after CARD_SEARCH_END"
      )
    )
  }
}
