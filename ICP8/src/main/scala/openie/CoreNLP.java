package openie;

import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import edu.stanford.nlp.util.Quadruple;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by Mayanka on 27-Jun-16.
 */
public class CoreNLP {
    public static String returnTriplets(String sentence) {

        Document doc = new Document(sentence);
        String triplets = "";
        for (Sentence sent : doc.sentences()) {  // Will iterate over two sentences

            //triplets+=sent+",";
            Iterator<Quadruple<String, String, String, Double>> openIETripletList = sent.openie().iterator();
            int noOfTriplets = 0;
            while (openIETripletList.hasNext()) {
                //triplets+= l.toString();
                Quadruple<String, String, String, Double> singleTriplet = openIETripletList.next();
                String subject = singleTriplet.first;
                String predicate = singleTriplet.second;
                String object = singleTriplet.third;
                // subject;object;predicate;\n
                if (subject.contains("model") ||
                        subject.contains("dataset") || object.contains("dataset")||
                        subject.contains("accuracy") || object.contains("accuracy") ||
                        subject.contains("epoch") || object.contains("epoch") ||
                        subject.contains("batch") || object.contains("batch") ||
                        subject.contains("learning") || object.contains("learning") ||
                        subject.contains("optimizer") || object.contains("optimizer")) {
                    triplets += subject + ";" + predicate + ";" + object + "\n";
                    noOfTriplets++;
                }
                // String modelName = "";
                // triplets += modelName + " model details ends here" + "\n";
            }
            // System.out.println(triplets);
            //triplets+=","+noOfTriplets+"\n";
        }
        /**
         *  Triplet Results
         *  sentence, subject;predicate;object\n subject;predicate;object \n , No. Of Triplets
         */
        return triplets + "\n";
    }

}
