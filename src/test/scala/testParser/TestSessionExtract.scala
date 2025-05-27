package testParser

import org.mockito.Mockito.mock
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testtask.parser.Session
import org.testtask.parser.processors.{ErrorStatsAccumulator, ParsingContext}

import java.time.LocalDateTime

class TestSessionExtract extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  var errorStatsAcc: ErrorStatsAccumulator = _

  override def beforeEach(): Unit = {
    errorStatsAcc = mock(classOf[ErrorStatsAccumulator])
    super.beforeEach()
  }

  "extract" should "build Session with valid session start and end" in {
    val lines = List(
      "SESSION_START 13.02.2020_21:45:55",
      "CARD_SEARCH_START Thu,_13_Feb_2020_21:45:56_+0300",
      "$134 запрос1",
      "CARD_SEARCH_END",
      "1CS1 DOC_123 DOC_456 DOC_1234",
      "DOC_OPEN 13.02.2020_21:45:57 1CS1 DOC_123",
      "DOC_OPEN 13.02.2020_21:45:58 1CS1 DOC_456",
      "QS 13.02.2020_22:45:59 {налогообложение}",
      "1QS1 DOC_567",
      "DOC_OPEN 13.02.2020_22:46:00 1QS1 DOC_567",
      "DOC_OPEN 13.02.2020_22:46:01 1CS1 DOC_1234",
      "CARD_SEARCH_START 13.02.2020_22:47:00",
      "$0 DOC_321",
      "CARD_SEARCH_END",
      "1CS2 DOC_321",
      "DOC_OPEN 13.02.2020_22:47:00 1CS2 DOC_321",
      "QS 13.02.2020_22:47:02 {пу пу пу}",
      "1QS2 DOC_765",
      "DOC_OPEN  1QS2 DOC_765",
      "DOC_OPEN 13.02.2020_22:48:00 11 DOC_000",
      "SESSION_END 13.02.2020_23:46:01"
    ).iterator.buffered

    val context = ParsingContext(lines, errorStatsAcc, "4")
    val session = Session.extract(context)

    session.sessionId shouldBe "4"
    session.sessionStart shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 45, 55))
    session.sessionEnd shouldBe Some(LocalDateTime.of(2020, 2, 13, 23, 46, 1))

    session.cardSearches.size shouldBe 2

    val cs1 = session.cardSearches.find(_.searchId == "1CS1").get
    cs1.queriesTexts shouldBe Seq("134 запрос1")
    cs1.relatedDocuments should contain allOf ("DOC_123", "DOC_456", "DOC_1234")
    cs1.docOpens.map(_.documentId) should contain allOf ("DOC_123", "DOC_456", "DOC_1234")

    val cs2 = session.cardSearches.find(_.searchId == "1CS2").get
    cs2.queriesTexts shouldBe Seq("0 DOC_321")
    cs2.relatedDocuments shouldBe Seq("DOC_321")
    cs2.docOpens.map(_.documentId) shouldBe Seq("DOC_321")

    session.quickSearches.size shouldBe 2

    val qs1 = session.quickSearches.find(_.searchId == "1QS1").get
    qs1.queryText shouldBe "налогообложение"
    qs1.relatedDocuments shouldBe Seq("DOC_567")
    qs1.docOpens.map(_.documentId) shouldBe Seq("DOC_567")

    val qs2 = session.quickSearches.find(_.searchId == "1QS2").get
    qs2.queryText shouldBe "пу пу пу"
    qs2.relatedDocuments shouldBe Seq("DOC_765")
    qs2.docOpens.map(_.documentId) shouldBe Seq("DOC_765")

    session.docOpens.size shouldBe 1
    session.docOpens.head.documentId shouldBe "DOC_000"
  }
}