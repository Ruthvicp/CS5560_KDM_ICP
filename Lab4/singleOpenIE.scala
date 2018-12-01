/*
Author : Ruthvic Punyamurtula
Date : Oct 18, 2018
 */
package openie

import java.io.PrintStream

import openie.openIE.toCamelCase
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object singleOpenIE {

  def main(args: Array[String]) {

    // For Windows Users
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    // Configuration
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val inputf = sc.wholeTextFiles("data/ReadmeFiles", 5)

    // Doing a total word count on the entire data set
    println("Total words from Keras Models : " + wordCount(inputf))

    // Map input in the format  tuple = (file_name,content) for each file
    //Doing this because, I will replace the model name for generating the over all triplets
    val input2 = inputf.map(f => (f._1.substring(f._1.lastIndexOf("/") + 1, f._1.length), f._2))
    val out1_overall_Triplets = new PrintStream("overall.csv")

    // finding triplets and writing the results to each file with the model name
    input2.collect().foreach(f => {
      val out2_eachfile_triplet = new PrintStream("results" + "/" + f._1)
      out2_eachfile_triplet.println(toCamelCase(CoreNLP.returnTriplets(f._2)))
      out2_eachfile_triplet.close()
    })
    out1_overall_Triplets.close()

    // reading all obtained triplets
    val inputTripletFiles = sc.wholeTextFiles("results", 5).cache()
    // performing the word count on these triplets
    println("Word count on extracted triplets : " + wordCount(inputTripletFiles))

    // if a triplet contains model in it, then we replace that model with the model file name
    val out3_Replaced_triplets = inputTripletFiles.map(f =>
      (f._1.substring(f._1.lastIndexOf("/") + 1, f._1.length),
        if (f._2.contains("model"))
          f._2.replaceAll("model", f._1.substring(f._1.lastIndexOf("/") + 1, f._1.length))
        //else
        //f._2
      ))

    //val filteredModels = output2.filter(f => f._2.toString.contains(f._1))

    val out4_only_content = out3_Replaced_triplets.map(f => f._2)

    val filenames = out3_Replaced_triplets.map(f => f._1).collect().toList

    val out5_Consolidated_Triplets = new PrintStream("overall.csv")
    out4_only_content.collect().foreach(f => {
      out5_Consolidated_Triplets.println(f)
    })
    out5_Consolidated_Triplets.close()

    val output_tobe_filtered = sc.textFile("overall.csv")
    val results = output_tobe_filtered.filter(f => filenames.exists(f.contains))

    val out6_Result = new PrintStream("overall2.csv")
    results.collect().foreach(f => {
      out6_Result.println(f.toString.replaceAll(".txt", ""))
    })
    out6_Result.close()

    val for_ontology = sc.textFile("overall2.csv")
    val out7_triplet_ont_Constructor = new PrintStream("Triplets")
    val out8_ObjProperties = new PrintStream("ObjectProperties")
    val out9_Individuals = new PrintStream("Individuals")

    for_ontology.collect().foreach(f => {

      val noHashTags = f.replaceAll("[#$%&*)(+//!@~:}{|]","_")
        val split_each_line = noHashTags.split(";")
      out8_ObjProperties.println(split_each_line {1} + ",Subject,Object,Func")
//      out9_Individuals.println(split_each_line {0} + ",Subject")
//      out9_Individuals.println(split_each_line {2} + ",Object")
      out9_Individuals.println( "Subject," + split_each_line {0})
      out9_Individuals.println( "Object," + split_each_line {2} )
      out7_triplet_ont_Constructor.println(noHashTags.toString.replaceAll(";",",") + ",Obj")
    })
    out7_triplet_ont_Constructor.close()
    out8_ObjProperties.close()
    out8_ObjProperties.close()

  }

  // performing word count on the data
  def wordCount(inputfiles: RDD[(String, String)]): Long = {
    val wordcountTriplets = inputfiles.flatMap(x => x._2).distinct()
    wordcountTriplets.count()
  }

  // We convert the triplets to CamelCase strings in order to load
  // the data into OWL Compatible format
  def toCamelCase(phrase: String): String = {
    var result = phrase

    if (phrase.contains(" ")) {
      val words = phrase.split(" ")

      for (i <- 1 until words.length) {
        if (words(i).length > 1) {
          words(i) = words(i).capitalize
        }
      }
      result = words.mkString("").replaceAll("[.]", "")
    }
    result
  }

}
