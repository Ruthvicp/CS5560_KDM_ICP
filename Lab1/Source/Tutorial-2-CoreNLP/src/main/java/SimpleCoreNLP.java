/**
 * Created by Mayanka on 14-Jun-16.
 */
import edu.stanford.nlp.simple.*;

public class SimpleCoreNLP {
    public static void main(String[] args) {
        // Create a document. No computation is done yet.
        Document doc = new Document("Citing high fuel prices, United Airlines said Friday " +
                "it has increased fares by $6 per round trip on flights to some cities also " +
                "served by lower-cost carriers. American Airlines, a unit AMR, immediately " +
                "matched the move, spokesman Tim Wagner said. United, a unit of UAL, said the " +
                "increase took effect Thursday night and applies to most routes where it " +
                "competes against discount carriers, such as Chicago to Dallas and Atlanta" +
                " and Denver to San Francisco, Los Angeles and New York.");
        for (Sentence sent : doc.sentences()) {  // Will iterate over two sentences
            // We're only asking for words -- no need to load any models yet
            //System.out.println("The second word of the sentence '" + sent + "' is " + sent.word(1));
            // When we ask for the lemma, it will load and run the part of speech tagger
            //System.out.println("The third lemma of the sentence '" + sent + "' is " + sent.lemma(2));
            // When we ask for the parse, it will load and run the parser
            //System.out.println("The parse of the sentence '" + sent + "' is " + sent.parse());
            // ...
            System.out.println("The triplet extraction of the sentence '" + sent + "' is " + sent.openie());
        }
    }
}