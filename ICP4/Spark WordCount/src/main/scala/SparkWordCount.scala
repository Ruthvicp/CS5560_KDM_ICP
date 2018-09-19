

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Mayanka on 09-Sep-15.
 */
object SparkWordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val inputf=sc.wholeTextFiles("Input1")
    inputf.map(abs => {
      abs._1 //path
      abs._2 // file content
    })

    val wc = inputf.flatMap(line => {line._2.split(" ")}).map(word =>( word,1))

    //val wc=inputf.flatMap(line=>{line.split(" ")}).map(word=>(word,1))

    val output=wc.reduceByKey(_+_)

    output.saveAsTextFile("output")

    val o=output.collect()

    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word+" : "+count+"\n"

    }}

  }

}
