
import java.util.Properties
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, PartOfSpeechAnnotation, SentencesAnnotation, TokensAnnotation}
import org.apache.spark.{SparkConf, SparkContext}
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import org.apache.spark.mllib.feature.{HashingTF, IDF, Word2Vec}
import scala.collection.JavaConversions._
import rita.RiWordNet
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

object SparkWordCount {

   def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // reading the input directory
    val inputfiles = sc.wholeTextFiles("Input")

    // lemmatize the data
    val lemmatized = inputfiles.map(line => lemmatize(line._2))
    val flatLemma = lemmatized.flatMap(list => list)
    val lemmatizedSeq = flatLemma.map(list => List(list._1).toSeq)

    // count for all words and count for all parts of speech
    val wc = flatLemma.map(word =>(word._1 + "," + word._2, 1))
    val wcTotal = flatLemma.map(word =>("total", 1))
    val posCount = flatLemma.map(word => (word._2, 1))

    // count for wordnet words
    val wordnetCount = flatLemma.map(word =>
      if(new RiWordNet("WordNet-3.0").exists(word._1))
        (word._1 + "," + word._2, 1)
      else (word._1 + "," + word._2, 0))
    val wordnetCountTotal = flatLemma.map(word =>
      if(new RiWordNet("WordNet-3.0").exists(word._1))
        ("total", 1)
      else ("total", 0))

    //Creating an object of HashingTF Class
    val hashingTF = new HashingTF()

    //Creating Term Frequency of the document
    val tf = hashingTF.transform(lemmatizedSeq)
    tf.cache()

    val idf = new IDF().fit(tf)

    //Creating Inverse Document Frequency
    val tfidf = idf.transform(tf)

    val tfidfvalues = tfidf.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val values = ff(2).replace("]", "").replace(")", "").split(",")
      values
    })

    val tfidfindex = tfidf.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val indices = ff(1).replace("]", "").replace(")", "").split(",")
      indices
    })

    val tfidfData = tfidfindex.zip(tfidfvalues)

    var hm = new HashMap[String, Double]

    tfidfData.collect().foreach(f => {
      hm += f._1 -> f._2.toDouble
    })

    val mapp = sc.broadcast(hm)

    val documentData = lemmatizedSeq.flatMap(_.toList)

    val docMap = documentData.map(f => {
      val i = hashingTF.indexOf(f)
      val h = mapp.value
      (f, h(i.toString))
    })

    val docMapSorted = docMap.distinct().sortBy(_._2, false)

    val word2vec = new Word2Vec().setVectorSize(1000)

    val model = word2vec.fit(lemmatizedSeq)

    //I took this from Cameron LeCuyer
    // reference: https://stackoverflow.com/questions/4089537/scala-catching-an-exception-within-a-map
    val wordVec = docMapSorted.collect().map( word =>
      try{Left((word._1, model.findSynonyms(word._1, 5)))}
      catch{case e: IllegalStateException => Right(e)})

    val (synonym, errors) = wordVec.partition {_.isLeft}
    val synonyms = synonym.map(_.left.get)

    docMapSorted.saveAsTextFile("Output\\LemmaTFIDF")

    val data = sc.parallelize(synonyms.map(word => word._1 + ":" + word._2.mkString(",")).toSeq)
    data.saveAsTextFile("Output\\LemmaW2V")

    val wNetCount = wordnetCount.reduceByKey(_+_)
    wNetCount.saveAsTextFile("Output\\WordNetCount")

    val oPOSCount = posCount.reduceByKey(_+_)
    oPOSCount.saveAsTextFile("Output\\PosCount")

    val wc_output = wc.reduceByKey(_+_)
    wc_output.saveAsTextFile("Output\\WordCount")

    val outTotal = wcTotal.reduceByKey(_+_)
    outTotal.saveAsTextFile("Output\\OutputTotal")

    val outWordnetTotal = wordnetCountTotal.reduceByKey(_+_)
    outWordnetTotal.saveAsTextFile("Output\\OutputWordnetTotal")
  } // end main

  // lemmatizes input string
  // code referenced from https://stackoverflow.com/questions/30222559/simplest-method-for-text-lemmatization-in-scala-and-spark
  def lemmatize(text: String): ListBuffer[(String, String)] = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP(props)
    val document = new Annotation(text)
    pipeline.annotate(document)
    /*val stopwords = List("a", "an", "the", "and", "are", "as", "at", "be", "by", "for", "from", "has", "in", "is",
      "it", "its", "of", "on", "that", "to", "was", "were", "will", "with", "''", "``")*/

    val lemmas = ListBuffer.empty[(String, String)]
    val sentences = document.get(classOf[SentencesAnnotation])

    // for each token get the lemma and part of speech
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      val pos = token.get(classOf[PartOfSpeechAnnotation])

      if (lemma.length > 1) {
        lemmas += ((lemma.toLowerCase, pos.toLowerCase))
      } // end if
    } // end loop
    lemmas
  } // end lemmatize

  // gets data for medical words from URL
  def get(url: String) = scala.io.Source.fromURL(url).getLines()

}
