package org.james_world

import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
//import java.time.LocalDate

object Main {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("ConsultantPlusLogs").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val filePath = "src/main/resources/data"
        val files = new File(filePath).listFiles()
        val filesRDD = sc.parallelize(files)

        val searchAnalyzer = new DocumentSearchAnalyzer("ACC_45616")
        val openAnalyzer = new DocumentOpenAnalyzer
        val searchResults = searchAnalyzer.analyze(filesRDD)
        val openResults = openAnalyzer.analyze(filesRDD)
        val FileSaver = new FileSaver()

        println(searchResults)
        FileSaver.saveResultsToCSV(openResults, "data\\results\\open_results.csv")

        sc.stop()
    }
}
