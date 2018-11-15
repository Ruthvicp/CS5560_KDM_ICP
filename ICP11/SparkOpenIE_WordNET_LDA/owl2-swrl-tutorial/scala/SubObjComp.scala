package openie

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SubObjComp {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    // Configuration
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val inputf = sc.textFile("data/test.txt")

    inputf.collect().foreach(f => {
      val line = f.split(',')
      val sub = line {0}
      val pred = line {1}
      //println(sub + ',' + pred)

      findMatch(sub,pred,inputf)

//      inputf.foreach(y => {
//        val line = f.split(',')
//        if (line {
//          2
//        }.compareTo(sub) == 0) {
//          if (line {
//            1
//          }.compareTo(pred) != 0) {
//            println(line {
//              1
//            })
//          }
//        }
//
//      })

    })

  }

  def findMatch(sub:String, pred:String, inputf: RDD[String]): Unit = {
    inputf.collect().foreach(y => {
      val line = y.split(',')
      if (line {2}.compareTo(sub) == 0) {
        if (line {1}.compareTo(pred) != 0) {
          println(y)
        }
      }
  })
  }
}
