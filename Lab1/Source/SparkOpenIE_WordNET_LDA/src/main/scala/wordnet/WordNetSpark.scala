package wordnet

import org.apache.spark.{SparkConf, SparkContext}
import rita.RiWordNet

/**
  * Created by Mayanka on 26-06-2017.
  */
object WordNetSpark {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setAppName("WordNetSpark").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)


    val data=sc.textFile("data/pubmed/1.txt")

    val dd=data.map(line=> {
      val wordnet = new RiWordNet("C:\\Users\\ruthv\\IdeaProjects\\Tutorial 3 Source Code\\SparkOpenIE_WordNET_LDA\\WordNet-3.0")
      //val farr = line.split(" ")
      val wordSet = line.split(" ")
      val synarr = wordSet.map(word => {
        if (wordnet.exists(word))
          (word, getSynoymns(wordnet, word))
        else
          (word, null)
      })
      synarr
    })
    dd.collect().foreach(linesyn=>{
      linesyn.foreach(wordssyn=>{
        if(wordssyn._2 != null)
          println(wordssyn._1+":"+wordssyn._2.mkString(","))
      })
    })
     // getSynoymns(wordnet,"allergy")
    //})
    //dd.take(1).foreach(f=>println(f.mkString(" ")))
  }
  def getSynoymns(wordnet:RiWordNet,word:String): Array[String] ={
    println(word)
    val pos=wordnet.getPos(word)
    println(pos.mkString(" "))
    //println("synonyms for the word are :")
    val syn=wordnet.getAllSynonyms(word, pos(0), 10)
    syn
  }

}
