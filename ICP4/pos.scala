
import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

object SparkPOSCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "2g")

    val sc=new SparkContext(sparkConf)

    val inputf = sc.wholeTextFiles("Input1", 100)
    val inputData = inputf.map(abs => {
      abs._2
    }).cache()

    // val inputData=sc.textFile("inputData", 10)

    val wc=inputData.flatMap(abstracts=> {abstracts.split("\n")}).map(singleAbs => {
      CoreNLP.returnSentences(singleAbs)
    }).map(sentences => {
      CoreNLP.returnPOS(sentences)
    }).flatMap(wordPOSLines => {
      wordPOSLines.split("\n")
    }).map(wordPOSPair => {
      wordPOSPair.split("\t")
    }).map(wordPOS => (wordPOS(1), 1)).cache()

    val output = wc.reduceByKey(_+_)

    output.saveAsTextFile("output")

    val o=output.collect()

    var nouns = 0
    var verbs = 0

    o.foreach{case(word,count)=> {

      if(word.contains("NN")) {
        nouns += count
      }
      else if(word.contains("VB")) {
        verbs += count
      }

    }}

    val nounVerbWriter = new PrintWriter(new File("finalData/nouns&verbs.txt"))
    nounVerbWriter.write("Total Nouns: " + nouns + "\nTotal Verbs: " + verbs)
    nounVerbWriter.close()


  }

}