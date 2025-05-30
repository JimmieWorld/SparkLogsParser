package testParser.testEvents

import org.testtask.parser.events.CardSearch
import org.testtask.parser.processors.{ErrorStatsAccumulator, ParsingContext}
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

  def extract(lines: List[String]): ParsingContext = {
    val bufferedIt = lines.iterator.buffered
    val context = ParsingContext(bufferedIt, errorStatsAcc, "4")
    CardSearch.parse(context)
    context
  }

  "CardSearch.parse" should "parse a simple card search with one query line and result" in {
    val lines = List(
      "CARD_SEARCH_START 13.02.2020_21:59:25",
      "$134 основные средства федеральный стандарт",
      "CARD_SEARCH_END",
      "11104369 PKBO_31048 LAW_283870"
    )

    val context = extract(lines)
    val cardSearch = context.sessionBuilder.cardSearches.head

    cardSearch.dateTime shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 59, 25))
    cardSearch.queriesTexts should contain theSameElementsAs Seq("134 основные средства федеральный стандарт")
    cardSearch.searchResult.searchId shouldEqual "11104369"
    cardSearch.searchResult.relatedDocuments should contain allOf ("PKBO_31048", "LAW_283870")

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

    val context = extract(lines)
    val cardSearch = context.sessionBuilder.cardSearches.head

    cardSearch.queriesTexts should contain theSameElementsAs Seq(
      "134 основные средства федеральный стандарт",
      "135 налог на имущество"
    )
    cardSearch.searchResult.searchId shouldEqual "11104369"
    cardSearch.searchResult.relatedDocuments should contain allOf ("PBI_238073", "LAW_344754")

    verifyZeroInteractions(errorStatsAcc)
  }

//  it should "parse without result line after CARD_SEARCH_END" in {
//    val lines = List(
//      "CARD_SEARCH_START 13.02.2020_21:59:25",
//      "$134 основные средства федеральный стандарт",
//      "CARD_SEARCH_END"
//    )
//
//    val context = extract(lines)
//    val cardSearch = context.sessionBuilder.cardSearches.head
//
//    cardSearch.queriesTexts should contain theSameElementsAs Seq("134 основные средства федеральный стандарт")
//    cardSearch.searchResult.searchId shouldEqual ""
//    cardSearch.searchResult.relatedDocuments shouldEqual Seq.empty
//
//    verify(errorStatsAcc).add(
//      (
//        "Warning: CardSearchMissingSearchResult",
//        "[file 4] Expected search result line after CARD_SEARCH_END"
//      )
//    )
//  }
}
