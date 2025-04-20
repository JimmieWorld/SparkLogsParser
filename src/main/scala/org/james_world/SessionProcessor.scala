package org.james_world

import java.time.LocalDate

object SessionProcessor {
    /**
     * Метод для подсчета количества раз, когда искали определенный документ в карточке поиска.
     *
     * @param documentId Идентификатор документа (например, "ACC_45616").
     * @param session объект сессии пользователя из лога
     * @return Количество раз, когда искали указанный документ.
     */
    def countSearchesForDocument(session: Session, documentId: String): Seq[(String, Long)] = {
        val countSearches = session.events.collect {
            case cs: CardSearch if cs.queriesTexts.exists(_.contains(documentId)) => 1
        }.sum
        Seq((documentId, countSearches))
    }

    /**
     * Метод для подсчета количества открытий документов, найденных через быстрый поиск, за каждый день.
     *
     * @param session объект сессии пользователя из лога
     * @return Коллекция пар ((date, documentId), count).
     */
    def countDocumentOpens(session: Session): Seq[((LocalDate, String), Int)] = {
        val quickSearchData = session.events.collect {
            case qs: QuickSearch => (qs.timestamp.toLocalDate, qs.relatedDocuments)
        }

        val foundDocumentsByDate = quickSearchData
            .groupBy(_._1)
            .view
            .mapValues(entries => entries.flatMap(_._2).toSet)
            .toMap

        val documentOpens = session.events.collect {
            case docOpen: DocumentOpen =>
                val date = docOpen.timestamp.toLocalDate
                (date, docOpen.documentId)
        }

        documentOpens
            .groupBy(identity)
            .collect {
                case ((date, documentId), count) if foundDocumentsByDate.getOrElse(date, Set.empty).contains(documentId) =>
                    val key = (date, documentId)
                    val value = count.size
                    (key, value)
            }.toSeq
    }
}