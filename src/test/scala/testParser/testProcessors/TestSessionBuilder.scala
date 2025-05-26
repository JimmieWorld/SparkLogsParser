package testParser.testProcessors

import org.mockito.Mockito.mock
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testTask.parser.processors._
import org.testTask.parser.events._

import java.time.LocalDateTime
import scala.collection.mutable

class TestSessionBuilder extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  var errorStatsAcc: ErrorStatsAccumulator = _

  override def beforeEach(): Unit = {
    errorStatsAcc = mock(classOf[ErrorStatsAccumulator])
    super.beforeEach()
  }

  def makeContext: ParsingContext = {
    val lines = Seq.empty.iterator.buffered
    ParsingContext(lines, errorStatsAcc, "4", mutable.Map.empty)
  }

  "SessionBuilder.build" should "attach DocumentOpen to QuickSearch by searchId" in {
    val context = makeContext
    val builder = SessionBuilder(context)

    val qs = QuickSearch(
      timestamp = Some(LocalDateTime.now()),
      searchId = "111",
      queryText = "query",
      relatedDocuments = Seq("DOC1"),
      docOpens = Seq.empty
    )

    val doo = DocumentOpen(
      timestamp = Some(LocalDateTime.now()),
      searchId = "111",
      documentId = "DOC1"
    )

    builder.addEvent(qs)
    builder.addEvent(doo)

    val session = builder.build("session1", None, None)

    session.quickSearches.head.docOpens shouldBe Seq(doo)
    session.docOpens shouldBe empty
  }

  it should "attach DocumentOpen to CardSearch by searchId" in {
    val context = makeContext
    val builder = SessionBuilder(context)

    val cs = CardSearch(
      timestamp = Some(LocalDateTime.now()),
      searchId = "123",
      queriesTexts = Seq("query"),
      relatedDocuments = Seq("DOC1"),
      docOpens = Seq.empty
    )

    val doo = DocumentOpen(
      timestamp = Some(LocalDateTime.now()),
      searchId = "123",
      documentId = "DOC1"
    )

    builder.addEvent(cs)
    builder.addEvent(doo)

    val session = builder.build("session1", None, None)

    session.cardSearches.head.docOpens shouldBe Seq(doo)
    session.docOpens shouldBe empty
  }

  it should "leave unmatched DocumentOpen in Session.docOpens" in {
    val context = makeContext
    val builder = SessionBuilder(context)

    val doo = DocumentOpen(
      timestamp = Some(LocalDateTime.now()),
      searchId = "12345aaaa",
      documentId = "DOC1"
    )

    builder.addEvent(doo)

    val session = builder.build("session1", None, None)

    session.docOpens shouldBe Seq(doo)
  }

  it should "handle mixed events and leave only unmatched docOpens" in {
    val context = makeContext
    val builder = SessionBuilder(context)

    val qs = QuickSearch(
      timestamp = Some(LocalDateTime.now()),
      searchId = "1QS1",
      queryText = "query",
      relatedDocuments = Seq("DOC1"),
      docOpens = Seq.empty
    )

    val cs = CardSearch(
      timestamp = Some(LocalDateTime.now()),
      searchId = "1CS1",
      queriesTexts = Seq("query"),
      relatedDocuments = Seq("DOC1"),
      docOpens = Seq.empty
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

    builder.addEvent(qs)
    builder.addEvent(cs)
    builder.addEvent(doo1)
    builder.addEvent(doo2)
    builder.addEvent(doo3)

    val session = builder.build("session1", None, None)

    session.quickSearches.head.docOpens shouldBe Seq(doo1)
    session.cardSearches.head.docOpens shouldBe Seq(doo2)
    session.docOpens shouldBe Seq(doo3)
  }

  it should "handle multiple CardSearch and QuickSearch with doc opens" in {
    val context = makeContext
    val builder = SessionBuilder(context)

    val cs1 = CardSearch(
      timestamp = Some(LocalDateTime.of(2023, 1, 1, 10, 0)),
      searchId = "1CS1",
      queriesTexts = Seq("query1"),
      relatedDocuments = Seq("DOC1"),
      docOpens = Seq.empty
    )

    val cs2 = CardSearch(
      timestamp = Some(LocalDateTime.of(2023, 1, 1, 10, 5)),
      searchId = "1CS2",
      queriesTexts = Seq("query2"),
      relatedDocuments = Seq("DOC2"),
      docOpens = Seq.empty
    )

    val qs1 = QuickSearch(
      timestamp = Some(LocalDateTime.of(2023, 1, 1, 10, 10)),
      searchId = "1QS1",
      queryText = "query3",
      relatedDocuments = Seq("DOC3"),
      docOpens = Seq.empty
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

    builder.addEvent(cs1)
    builder.addEvent(cs2)
    builder.addEvent(qs1)
    builder.addEvent(doo1)
    builder.addEvent(doo2)
    builder.addEvent(doo3)

    val session = builder.build("session-multi", None, None)

    session.cardSearches.find(_.searchId == "1CS1").get.docOpens shouldBe Seq(doo1)
    session.cardSearches.find(_.searchId == "1CS2").get.docOpens shouldBe Seq(doo2)
    session.quickSearches.find(_.searchId == "1QS1").get.docOpens shouldBe Seq(doo3)
    session.docOpens shouldBe empty
  }
}