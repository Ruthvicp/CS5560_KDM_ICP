import edu.stanford.nlp.coref.CorefCoreAnnotations;
import edu.stanford.nlp.coref.data.CorefChain;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Created by Mayanka on 13-Jun-16.
 */
public class Main {
    public static void main(String args[]) {
        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

// read some text in the text variable
        String text = "Although intranasal steroids and anti-cysteinyl-leukotriene-receptor antagonists are efficacious in the treatment of seasonal allergic rhinitis (SAR), combinations of these agents have not unequivocally been demonstrated to be superior to the individual drugs. We aimed to compare the efficacy and potential mechanisms of budesonide nasal spray (BD), oral montelukast (MNT), and combination therapy comprising a half-dose of budesonide plus montelukast (hBD+MNT) in SAR patients.\n" +
                "\n" +
                "The evidence supporting the prophylactic treatment of seasonal allergic rhinitis before the start of pollen dispersal is still lacking. We conducted a study to investigate the efficacy of prophylaxis with montelukast for seasonal allergic rhinitis and to evaluate its influence on the inflammatory condition of the lower airway. Our final study population was made up of 57 adults who were randomized to a prophylactic treatment group and a control group. The prophylaxis group was made up of 31 patients-10 men and 21 women, aged 18 to 54 years (mean: 36.9)-who were administered montelukast for 2 weeks before the cypress pollen season and subsequently throughout the remainder of the season. The control group was made up of 26 patients-11 men and 15 women, aged 24 to 63 years (mean: 39.2)-who took montelukast during the pollen season only. During the pollen season, the mean daily rescue medication score was significantly lower in the prophylaxis group (3.22 vs. 3.89; p = 0.001). However, there was no statistical difference in the two groups' mean daily rhinoconjunctivitis symptom scores. Also, the fraction of exhaled nitric oxide in the prophylaxis group tended to be lower than that of control group, but again the difference was not significant (29.8 vs. 42.1 ppb; p = 0.189). We conclude that antileukotriene prophylaxis started 2 weeks before the cypress pollen dispersal was effective in reducing the need for rescue medication during the pollen season and showed a trend toward alleviating the eosinophilic inflammation in the lower airway induced by the pollen.\n" +
                "\n" +
                "To review oral allergy syndrome (OAS)\n" +
                "\n" +
                "The Joint Task Force on Practice Parameters, which comprises representatives of the American Academy of Allergy, Asthma and Immunology (AAAAI) and the American College of Allergy, Asthma and Immunology (ACAAI), formed a workgroup to review evidence and provide guidance to health care providers on the initial pharmacologic treatment of seasonal allergic rhinitis in patients aged 12 years or older.\n" +
                "\n" +
                "Cetirizine has been shown to be effective for relief of seasonal allergic rhinitis (SAR) symptoms. Allergic rhinitis symptoms have been reported to have circadian variations, with symptoms tending to be most bothersome overnight and in the morning. "; // Add your text here!

// create an empty Annotation just with the given text
        Annotation document = new Annotation(text);
        System.out.println("Annotation step - " +document);

// run all Annotators on this text
        pipeline.annotate(document);

        // these are all the sentences in this document
// a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {

                System.out.println("\n" + token);

                // this is the text of the token
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                System.out.println("Text Annotation");
                System.out.println(token + ":" + word);
                // this is the POS tag of the token

                String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
                System.out.println("Lemma Annotation");
                System.out.println(token + ":" + lemma);
                // this is the Lemmatized tag of the token


                String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                System.out.println("POS");
                System.out.println(token + ":" + pos);

                // this is the NER label of the token
                String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                System.out.println("NER");
                System.out.println(token + ":" + ne);

                System.out.println("\n\n");
            }

            // this is the parse tree of the current sentence
            Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
            System.out.println(tree);
            // this is the Stanford dependency graph of the current sentence


            Map<Integer, CorefChain> graph =
                    document.get(CorefCoreAnnotations.CorefChainAnnotation.class);
            System.out.println(graph.values().toString());
        }

    }
}
