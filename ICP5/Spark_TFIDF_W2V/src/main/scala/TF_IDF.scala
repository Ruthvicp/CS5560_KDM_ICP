import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF, Word2Vec}
import edu.stanford.nlp.util.StringUtils
import collection.JavaConverters
import scala.collection.immutable.HashMap
import org.apache.spark.api.java.JavaRDD


object TF_IDF {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    //Reading the Text File
    //val documents = sc.textFile("data/Article.txt")
    val documents = sc.wholeTextFiles("Input1", 2)

    // We will perform TFIDF on original document words, lemmatized words and
    // n-gram based words

    //1. Original document
    val documentSeq = documents.map(f => {
      val splitString = f._2.split(" ")
      splitString.toSeq
    })

    //2. Lemmatised words in TextFile
    val documentLemmaSeq = documents.map(f => {
      val lemmatised = CoreNLP.returnLemma(f._2)
      val splitString = lemmatised.split(" ")
      splitString.toSeq
    })

    // 3. N-gram sequence
    val documentNGramSeq = documents.map(f => {
      val ngram = getNGrams(f._2, 3).map(list => list.mkString(" "))
      ngram
    })

    //Creating an object of HashingTF Class
    val hashingTF = new HashingTF()

    //uncomment the related document sequence to perfom TF_IDF on it

    //Creating Term Frequency of the document
    //val tf = hashingTF.transform(documentSeq)
    val tf = hashingTF.transform(documentLemmaSeq)
    //val tf = hashingTF.transform(documentNGramSeq)
    //tf.cache()

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

    //tfidf.foreach(f => println(f))

    val tfidfData = tfidfindex.zip(tfidfvalues)

    var hash = new HashMap[String, Double]

    tfidfData.collect().foreach(f => {
      hash += f._1 -> f._2.toDouble
    })

    val mapp = sc.broadcast(hash)

    val documentData = documentSeq.flatMap(_.toList)
    //val documentData = documentLemmaSeq.flatMap(_.toList)
    //val documentData = documentNGramSeq.flatMap(_.toList)

    val dd = documentData.map(f => {
      val i = hashingTF.indexOf(f)
      val h = mapp.value
      (f, h(i.toString))
    })

    val dd1 = dd.distinct().sortBy(_._2, false)
    /*dd1.take(20).foreach(f => {
      println(f)
    })*/


    //We will use the same program to find the word2vec based on the  1. original
    // document sequence 2. Lemmatized sequence 3. N-Gram based sequence
    val word2vec = new Word2Vec().setVectorSize(1000)

    // uncomment the below lines based on the input type
    val model = word2vec.fit(documentSeq)
    //val model = word2vec.fit(documentLemmaSeq)
    //val model = word2vec.fit(documentNGramSeq)

    // was stuck on this as my TF_IDF terms had some data like "0.21, +21" which had no
    // word available in the trained model's dictionary
    // so i took the below code from Cameron -  to fix the catch block
    // reference: https://stackoverflow.com/questions/4089537/scala-catching-an-exception-within-a-map
    val wordVec = dd1.take(20).map( word => try{Left(model.findSynonyms(word._1, 5))}
    catch{case e: IllegalStateException => Right(e)})

    val (synonym, errors) = wordVec.partition {_.isLeft}
    val synonyms = synonym.map(_.left.get)

    for (vec <- synonyms) {
      for((synonym, cosineSimilarity) <- vec)
      {
        println(s"$synonym $cosineSimilarity")
      }
    }
  }

  // reference: https://stackoverflow.com/questions/8258963/how-to-generate-n-grams-in-scala
  def getNGrams(sentence: String, n:Int): IndexedSeq[List[String]] = {
    val words = sentence
    //val ngrams = words.split(' ').sliding(n)
    // reference: https://stackoverflow.com/questions/8258963/how-to-generate-n-grams-in-scala
    val ngrams = (for(i <- 1 to n) yield words.split(' ').sliding(i).map(word => word.toList)).flatMap(x => x)
    ngrams
  }
}