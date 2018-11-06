package openie

import java.io.PrintStream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Mayanka on 27-Jun-16.
  */
object SparkOpenIE {

  def main(args: Array[String]) {

    // For Windows Users
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    // Configuration
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    // Turn off Info Logger for Console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val input = sc.wholeTextFiles("data/Rme", 2).map(line => {
      val triples = CoreNLP.returnTriplets(line._2.toString)
      triples
    }).distinct().collect()

    val stopwords = sc.textFile("data/Predicates.txt").collect()
    val stopwordBroadCast = sc.broadcast(stopwords)

    val input_No_StopWords = input.map(f=> {
      val afterStopWordRemoval = f.split(';')
      afterStopWordRemoval.toSeq
    })

  }
}



    /*    val in2 = input.map(line => {
        val arrylines = line.split(';') // {subject,pedictem;obj}
        arrylines(1).contains(stopwordBroadCast contains _)

       arrylines(1)
        val filteredlines = arrylines(f => {
          f.slice(1, 2).filter(stopwordBroadCast.value.contains(_))
        })
        filteredlines.toString
      line
      })

    val out= new PrintStream("TripletList1.csv")
    in2.foreach(f=>{
      out.println(f)
    })
    out.close()  */
/*
    val inputfiles = sc.wholeTextFiles("data/ReadmeFiles")
    // lemmatize the data

    val stopwords=sc.textFile("data/stopwords.txt").collect()
    val stopwordBroadCast=sc.broadcast(stopwords)

    val documentSeq = inputfiles.map(f => {
      val splitString = f._2.split(" ")
      splitString.toSeq
    })

    val input_No_StopWords = documentSeq.map(f=> {
      val afterStopWordRemoval = f.filter(!stopwordBroadCast.value.contains(_)).filter( w => !w.contains(","))
      afterStopWordRemoval.toSeq
    })

    val input = input_No_StopWords.map(line => {
        val t = CoreNLP.returnTriplets(line.toString)
        t
    }).distinct().collect()

    val out= new PrintStream("TripletList1.csv")
    input.foreach(f=>{
      out.println(f)
    })
out.close() */


