package openie

import java.io._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object openIE {
  def main(args: Array[String]) {

    // For Windows Users
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    // Configuration
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val inputf = sc.wholeTextFiles("data/ReadmeFiles", 2)
    val input = inputf.map(abs => {
      abs._2
    }).cache()


    val sent = input.map(l => (l.split("\n")))
    val filteredsent = sent.flatMap(f => {
      val content = f.filter(
        l => l.contains("trained") || l.contains("epoch") || l.contains("batch")
          || l.contains("learning rate") || l.contains("accuracy") || l.contains("model")
          || l.contains(".h5"))
      content
    }
    ).collect()

    val trip1 = filteredsent.map(line => {
      val ret = CoreNLP.returnTriplets(line.toString)
      ret
    }).distinct


    val out = new PrintStream("TripletList1.csv")
    trip1.foreach(f => {
      out.println(toCamelCase(f))
    })
    out.close()


    process(inputf)
  }


  def toCamelCase(phrase: String): String = {
    var temp = phrase

    if (phrase.contains(" ")) {
      val words = phrase.split(" ")

      for (i <- 1 until words.length) {
        if (words(i).length > 1) {
          words(i) = words(i).capitalize
        }
      }

      temp = words.mkString("").replaceAll("[.]", "")
    }

    temp
  }

  def process(inputf: RDD[(String, String)]){

    val input2 = inputf.map(f => {
      val name = f._1
      val path = "output"
      val out2 = new PrintStream("output" + "/" + name)
      val data = f._2.split("\n").map(line => {
        val value = line.filter(x => x.toString.contains("trained"))
        val triplets = CoreNLP.returnTriplets(value)
        triplets
      })
      data.foreach(f => {
        out2.println(toCamelCase(f))
      })
      out2.close()
    })

  }


}

/*

    val sen = input.collect().foreach(f => {
      val eachSent = f.split("\n")
      val filteredsent = eachSent.filter(a => a.contains("trained"))
     filteredsent.toSeq
      filteredsent.foreach(f => {
        val triples = CoreNLP.returnTriplets(f)
        triples
      })
    })
  }
}

} */