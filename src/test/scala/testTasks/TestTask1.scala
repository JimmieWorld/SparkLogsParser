package testTasks

import org.scalatest.BeforeAndAfterEach
import org.testtask.parser.events.{CardSearch, DocumentOpen, QuickSearch, SearchResult}
import org.testtask.parser.Session
import org.testtask.tasks.Task1
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayOutputStream, PrintStream}
import java.time.LocalDateTime

class TestTask1 extends AnyFlatSpec with Matchers with TestSparkContext with BeforeAndAfterEach {

  val sessionStart: Option[LocalDateTime] = Some(LocalDateTime.of(2023, 8, 1, 12, 0))
  val sessionEnd: Option[LocalDateTime] = Some(LocalDateTime.of(2023, 8, 1, 12, 5))

  var outputStream: ByteArrayOutputStream = _
  var systemOut: PrintStream = _

  override def beforeEach(): Unit = {
    // Сохраняем оригинальный stdout
    systemOut = System.out

    // Создаем буфер для перехвата вывода
    outputStream = new ByteArrayOutputStream()
    val captureOut = new PrintStream(outputStream)

    // Перенаправляем вывод
    System.setOut(captureOut)

    super.beforeEach()
  }

  override def afterEach(): Unit = {
    // Восстанавливаем stdout
    System.setOut(systemOut)
    // Очищаем буфер
    outputStream = null

    super.afterEach()
  }

  "Task1.execute" should "count only matching CardSearchEvents in multiple sessions" in {
    val session1 = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
          queriesTexts = Seq("0 ACC_45617"),
          searchResult = SearchResult("cs1", Seq("ACC_45617"))
        ),
        CardSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
          queriesTexts = Seq("0 ACC_45616"),
          searchResult = SearchResult("cs2", Seq("ACC_45616"))
        )
      ),
      quickSearches = Seq.empty,
      allDocOpens = Seq.empty
    )

    val session2 = Session(
      sessionId = "session2",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(LocalDateTime.of(2023, 8, 2, 10, 0)),
          queriesTexts = Seq("0 DOC_789"),
          searchResult = SearchResult("cs3", Seq("DOC_789"))
        )
      ),
      quickSearches = Seq.empty,
      allDocOpens = Seq.empty
    )

    Task1.execute(sc.parallelize(Seq(session1, session2)))

    val output = outputStream.toString
    output should include("Task1 document: ACC_45616 was found 1 times")
  }

  it should "return zero if no matching documents found" in {
    val session = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(sessionStart.get.plusMinutes(2)),
          queriesTexts = Seq("0 DOC_123", "0 DOC_456"),
          searchResult = SearchResult("cs1", Seq("DOC_123"))
        )
      ),
      quickSearches = Seq.empty,
      allDocOpens = Seq(
        DocumentOpen(
          timestamp = Some(sessionStart.get.plusMinutes(3)),
          searchId = "qs1",
          documentId = "ACC_45616"
        )
      )
    )

    Task1.execute(sc.parallelize(Seq(session)))

    val output = outputStream.toString
    output should include("Task1 document: ACC_45616 was found 0 times")
  }

  it should "ignore unrelated query texts and other event types" in {
    val session = Session(
      sessionId = "session3",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(sessionStart.get.plusMinutes(2)),
          queriesTexts = Seq("0 NOT_ACC_45616"),
          searchResult = SearchResult("cs1", Seq("ACC_45616"))
        )
      ),
      quickSearches = Seq(
        QuickSearch(
          timestamp = Some(sessionStart.get.plusMinutes(3)),
          queryText = "ACC_45616",
          searchResult = SearchResult("qs1", Seq("ACC_45616"))
        )
      ),
      allDocOpens = Seq.empty
    )

    Task1.execute(sc.parallelize(Seq(session)))

    val output = outputStream.toString
    output should include("Task1 document: ACC_45616 was found 0 times")
  }

  it should "count correctly across multiple sessions" in {
    val session1 = Session(
      sessionId = "session1",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(sessionStart.get.plusMinutes(2)),
          queriesTexts = Seq("0 ACC_45616"),
          searchResult = SearchResult("cs1", Seq("ACC_45616"))
        )
      ),
      quickSearches = Seq.empty,
      allDocOpens = Seq.empty
    )

    val session2 = Session(
      sessionId = "session2",
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = Seq(
        CardSearch(
          timestamp = Some(sessionStart.get.plusMinutes(3)),
          queriesTexts = Seq("0 ACC_45616"),
          searchResult = SearchResult("cs2", Seq("ACC_45616"))
        ),
        CardSearch(
          timestamp = Some(sessionStart.get.plusMinutes(3)),
          queriesTexts = Seq("0 ACC_45616", "0 DOC_123"),
          searchResult = SearchResult("cs3", Seq("DOC_123"))
        )
      ),
      quickSearches = Seq.empty,
      allDocOpens = Seq.empty
    )

    Task1.execute(sc.parallelize(Seq(session1, session2)))

    val output = outputStream.toString
    output should include("Task1 document: ACC_45616 was found 3 times")
  }
}
