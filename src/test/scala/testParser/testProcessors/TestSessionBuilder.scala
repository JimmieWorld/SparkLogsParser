package testParser.testProcessors

import org.mockito.Mockito.mock
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testtask.parser.processors._
import org.testtask.parser.events._

import java.time.LocalDateTime

class TestSessionBuilder extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  var errorStatsAcc: ErrorStatsAccumulator = _

  override def beforeEach(): Unit = {
    errorStatsAcc = mock(classOf[ErrorStatsAccumulator])
    super.beforeEach()
  }

  "SessionBuilder.build" should "attach DocumentOpen to QuickSearch by searchId" in {
    val builder = SessionBuilder("4")

    val qs = QuickSearch(
      timestamp = Some(LocalDateTime.now()),
      queryText = "query",
      searchResult = SearchResult("111", Seq("DOC1"))
    )

    val doo = DocumentOpen(
      timestamp = Some(LocalDateTime.now()),
      searchId = "111",
      documentId = "DOC1"
    )

    builder.quickSearches :+= qs
    builder.docOpens :+= doo

    val session = builder.build()

    session.quickSearches.head.searchResult.docOpens shouldBe Seq(doo)
    session.allDocOpens.size shouldBe 1
  }

  it should "attach DocumentOpen to CardSearch by searchId" in {
    val builder = SessionBuilder("4")

    val cs = CardSearch(
      timestamp = Some(LocalDateTime.now()),
      queriesTexts = Seq("query"),
      searchResult = SearchResult("123", Seq("DOC1"))
    )

    val doo = DocumentOpen(
      timestamp = Some(LocalDateTime.now()),
      searchId = "123",
      documentId = "DOC1"
    )

    builder.cardSearches :+= cs
    builder.docOpens :+= doo

    val session = builder.build()

    session.cardSearches.head.searchResult.docOpens shouldBe Seq(doo)
    session.allDocOpens.size shouldBe 1
  }

  it should "handle mixed events and leave only unmatched docOpens" in {
    val builder = SessionBuilder("4")

    val qs = QuickSearch(
      timestamp = Some(LocalDateTime.now()),
      queryText = "query",
      searchResult = SearchResult("1QS1", Seq("DOC1"))
    )

    val cs = CardSearch(
      timestamp = Some(LocalDateTime.now()),
      queriesTexts = Seq("query"),
      searchResult = SearchResult("1CS1", Seq("DOC1"))
    )

    val doo1 = DocumentOpen(
      timestamp = Some(LocalDateTime.now()),
      searchId = "1QS1",
      documentId = "DOC1"
    )

    val doo2 = DocumentOpen(
      timestamp = Some(LocalDateTime.now()),
      searchId = "1CS1",
      documentId = "DOC1"
    )

    val doo3 = DocumentOpen(
      timestamp = Some(LocalDateTime.now()),
      searchId = "1UNKNOWN",
      documentId = "DOC2"
    )

    builder.quickSearches :+= qs
    builder.cardSearches :+= cs
    builder.docOpens ++= Seq(doo1, doo2, doo3)

    val session = builder.build()

    session.quickSearches.head.searchResult.docOpens shouldBe Seq(doo1)
    session.cardSearches.head.searchResult.docOpens shouldBe Seq(doo2)
    session.allDocOpens shouldBe Seq(doo3)
  }

  it should "handle multiple CardSearch and QuickSearch with doc opens" in {
    val builder = SessionBuilder("4")

    val cs1 = CardSearch(
      timestamp = Some(LocalDateTime.of(2023, 1, 1, 10, 0)),
      queriesTexts = Seq("query1"),
      searchResult = SearchResult("1CS1", Seq("DOC1"))
    )

    val cs2 = CardSearch(
      timestamp = Some(LocalDateTime.of(2023, 1, 1, 10, 5)),
      queriesTexts = Seq("query2"),
      searchResult = SearchResult("1CS2", Seq("DOC2"))
    )

    val qs1 = QuickSearch(
      timestamp = Some(LocalDateTime.of(2023, 1, 1, 10, 10)),
      queryText = "query3",
      searchResult = SearchResult("1QS1", Seq("DOC3"))
    )

    val doo1 = DocumentOpen(
      timestamp = Some(LocalDateTime.of(2023, 1, 1, 10, 15)),
      searchId = "1CS1",
      documentId = "DOC1"
    )

    val doo2 = DocumentOpen(
      timestamp = Some(LocalDateTime.of(2023, 1, 1, 10, 20)),
      searchId = "1CS2",
      documentId = "DOC2"
    )

    val doo3 = DocumentOpen(
      timestamp = Some(LocalDateTime.of(2023, 1, 1, 10, 25)),
      searchId = "1QS1",
      documentId = "DOC3"
    )

    builder.cardSearches ++= Seq(cs1, cs2)
    builder.quickSearches :+= qs1
    builder.docOpens ++= Seq(doo1, doo2, doo3)

    val session = builder.build()

    session.cardSearches.find(_.searchResult.searchId == "1CS1").get.searchResult.docOpens shouldBe Seq(doo1)
    session.cardSearches.find(_.searchResult.searchId == "1CS2").get.searchResult.docOpens shouldBe Seq(doo2)
    session.quickSearches.find(_.searchResult.searchId == "1QS1").get.searchResult.docOpens shouldBe Seq(doo3)
    session.allDocOpens shouldBe empty
  }
}