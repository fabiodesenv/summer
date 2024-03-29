package br.edu.ufam.icomp.utils;

import java.text.Normalizer;
import java.util.HashSet;
import java.util.Set;

public final class TextAnalysisUtil {

    //private static final String SPLIT_REGEX = "(\\W|\\s|\\d)";
	private static final String SPLIT_REGEX = "(\\W|\\s)";
	private static Set<String> m_Words = new HashSet<String>();

    private TextAnalysisUtil() {
    	startStopwords();
    }

    /**
     * This method validates a term according to the following conditions:
     * - Term is not null or empty
     * - Length of term > 1
     * - Term is not a number
     * - Term does not start with a digit
     * - Term is not all numeric
     * - Term is bigger than minimum size
     * - Term is smaller than maximum size
     * - It is not a stop word
     *
     * @param term
     * @return true if term is valid
     *
     */
    public static boolean isValidTerm(String term, Boolean removeStopWords) {
    	
    	if (!removeStopWords) {
    		return ( !StringHelper.isNullOrEmpty(term) && term.length() <= Constants.MAXIMUM_WORD_SIZE &&
                    !StringHelper.isLong(term) && !Character.isDigit(term.charAt(0)) &&
                    !StringHelper.isNumeric(term) && term.length() >= Constants.MINIMUM_WORD_SIZE
                   );
    	}
    	
        return ( !StringHelper.isNullOrEmpty(term) && term.length() <= Constants.MAXIMUM_WORD_SIZE &&
                 !StringHelper.isLong(term) && !Character.isDigit(term.charAt(0)) &&
                 !StringHelper.isNumeric(term) && term.length() >= Constants.MINIMUM_WORD_SIZE &&
                 !m_Words.contains(term)
                );
    	
    }
    
    public static boolean isValidTerm(String term) {
    	return isValidTerm(term, true);
    }

    /**
     * This method takes the given text, replaces all '_' in it by ' ', and tokenizes the text.
     *
     * @param text Text to be tokenized.
     * @return tokens An array of tokens.
     *
     */
    public static String[] tokenize(String text) {
        
        return text.replace('_', ' ').split(SPLIT_REGEX);

    }
    
    /**
     * This method receives a string as input parameter and normalizes it to 
     * lowercase, removes leading and trailing spaces and strips accents from 
     * the input string. 
     * 
     * @param item string to be normalized
     * 
     * @return normalized string 
     */
    public static String normalizeString(String item) {
        
        // remove invalid characters
        String result = removeInvalidCharacters(item);
        
        // remove leading and trailing spaces, normalize to lowercase, decompose string 
        result = Normalizer.normalize(result.trim().toLowerCase(),Normalizer.Form.NFKD);
        
        //remove the accent characters from the decomposed string using a regular expression
        result = result.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
        
        //remove posible middle white spaces
        result = result.replaceAll("([\\s]+)", " ");

        return result;        
    }
    
    /**
     * This methods removes from string: \n \t ; |
     * @param sentence
     * @return string without invalid characters
     */
    public static String removeInvalidCharacters(String sentence) {
        String result = sentence.replaceAll("\\n|\\t|\"|;|\\|", "");
        return result.replaceAll("&nbsp", " "); 
    }
    
    public static void main(String[] args) {
        
        String test = "Moves \" Like Denzel Washington Gangster; feat >>> Marron 5 | Christina Aguilera"; 
        
        test = normalizeString(test);
        
        System.out.println(test);
    }

    public static void startStopwords() {

		// Stopwords list from Rainbow
		m_Words.add("a");
		m_Words.add("able");
		m_Words.add("about");
		m_Words.add("the");
		m_Words.add("above");
		m_Words.add("according");
		m_Words.add("accordingly");
		m_Words.add("across");
		m_Words.add("actually");
		m_Words.add("after");
		m_Words.add("afterwards");
		m_Words.add("again");
		m_Words.add("against");
		m_Words.add("all");
		m_Words.add("allow");
		m_Words.add("allows");
		m_Words.add("almost");
		m_Words.add("alone");
		m_Words.add("along");
		m_Words.add("already");
		m_Words.add("also");
		m_Words.add("although");
		m_Words.add("always");
		m_Words.add("am");
		m_Words.add("among");
		m_Words.add("amongst");
		m_Words.add("an");
		m_Words.add("and");
		m_Words.add("another");
		m_Words.add("any");
		m_Words.add("anybody");
		m_Words.add("anyhow");
		m_Words.add("anyone");
		m_Words.add("are");
		m_Words.add("around");
		m_Words.add("as");
		m_Words.add("aside");
		m_Words.add("ask");
		m_Words.add("asking");
		m_Words.add("at");
		m_Words.add("away");
		m_Words.add("be");
		m_Words.add("became");
		m_Words.add("because");
		m_Words.add("become");
		m_Words.add("becomes");
		m_Words.add("becoming");
		m_Words.add("been");
		m_Words.add("before");
		m_Words.add("beforehand");
		m_Words.add("behind");
		m_Words.add("being");
		m_Words.add("believe");
		m_Words.add("below");
		m_Words.add("beside");
		m_Words.add("besides");
		m_Words.add("best");
		m_Words.add("better");
		m_Words.add("between");
		m_Words.add("beyond");
		m_Words.add("both");
		m_Words.add("brief");
		m_Words.add("but");
		m_Words.add("by");
		m_Words.add("c");
		m_Words.add("came");
		m_Words.add("can");
		m_Words.add("cannot");
		m_Words.add("cant");
		m_Words.add("cause");
		m_Words.add("causes");
		m_Words.add("certain");
		m_Words.add("certainly");
		m_Words.add("changes");
		m_Words.add("clearly");
		m_Words.add("co");
		m_Words.add("com");
		m_Words.add("come");
		m_Words.add("comes");
		m_Words.add("concerning");
		m_Words.add("consequently");
		m_Words.add("consider");
		m_Words.add("considering");
		m_Words.add("contain");
		m_Words.add("containing");
		m_Words.add("contains");
		m_Words.add("corresponding");
		m_Words.add("could");
		m_Words.add("course");
		m_Words.add("currently");
		m_Words.add("d");
		m_Words.add("de");
		m_Words.add("definitely");
		m_Words.add("described");
		m_Words.add("despite");
		m_Words.add("did");
		m_Words.add("different");
		m_Words.add("do");
		m_Words.add("does");
		m_Words.add("doing");
		m_Words.add("done");
		m_Words.add("down");
		m_Words.add("downwards");
		m_Words.add("during");
		m_Words.add("e");
		m_Words.add("each");
		m_Words.add("edu");
		m_Words.add("eg");
		m_Words.add("eight");
		m_Words.add("either");
		m_Words.add("else");
		m_Words.add("elsewhere");
		m_Words.add("enough");
		m_Words.add("entirely");
		m_Words.add("especially");
		m_Words.add("et");
		m_Words.add("etc");
		m_Words.add("even");
		m_Words.add("ever");
		m_Words.add("every");
		m_Words.add("everybody");
		m_Words.add("everyone");
		m_Words.add("everything");
		m_Words.add("everywhere");
		m_Words.add("ex");
		m_Words.add("exactly");
		m_Words.add("example");
		m_Words.add("except");
		m_Words.add("f");
		m_Words.add("far");
		m_Words.add("few");
		m_Words.add("fifth");
		m_Words.add("first");
		m_Words.add("five");
		m_Words.add("followed");
		m_Words.add("following");
		m_Words.add("follows");
		m_Words.add("for");
		m_Words.add("former");
		m_Words.add("formerly");
		m_Words.add("forth");
		m_Words.add("four");
		m_Words.add("from");
		m_Words.add("further");
		m_Words.add("furthermore");
		m_Words.add("g");
		m_Words.add("get");
		m_Words.add("gets");
		m_Words.add("getting");
		m_Words.add("given");
		m_Words.add("gives");
		m_Words.add("go");
		m_Words.add("goes");
		m_Words.add("going");
		m_Words.add("got");
		m_Words.add("gotten");
		m_Words.add("greetings");
		m_Words.add("h");
		m_Words.add("had");
		m_Words.add("happens");
		m_Words.add("hardly");
		m_Words.add("has");
		m_Words.add("have");
		m_Words.add("having");
		m_Words.add("he");
		m_Words.add("hello");
		m_Words.add("help");
		m_Words.add("hence");
		m_Words.add("her");
		m_Words.add("here");
		m_Words.add("hereafter");
		m_Words.add("hereby");
		m_Words.add("herein");
		m_Words.add("hereupon");
		m_Words.add("hers");
		m_Words.add("herself");
		m_Words.add("hi");
		m_Words.add("him");
		m_Words.add("himself");
		m_Words.add("his");
		m_Words.add("hither");
		m_Words.add("hopefully");
		m_Words.add("how");
		m_Words.add("howbeit");
		m_Words.add("however");
		m_Words.add("i");
		m_Words.add("ie");
		m_Words.add("if");
		m_Words.add("ignored");
		m_Words.add("immediate");
		m_Words.add("in");
		m_Words.add("inasmuch");
		m_Words.add("inc");
		m_Words.add("indeed");
		m_Words.add("indicate");
		m_Words.add("indicated");
		m_Words.add("indicates");
		m_Words.add("inner");
		m_Words.add("insofar");
		m_Words.add("instead");
		m_Words.add("into");
		m_Words.add("inward");
		m_Words.add("is");
		m_Words.add("it");
		m_Words.add("its");
		m_Words.add("itself");
		m_Words.add("j");
		m_Words.add("just");
		m_Words.add("k");
		m_Words.add("keep");
		m_Words.add("keeps");
		m_Words.add("kept");
		m_Words.add("know");
		m_Words.add("knows");
		m_Words.add("known");
		m_Words.add("l");
		m_Words.add("la");
		m_Words.add("last");
		m_Words.add("lately");
		m_Words.add("later");
		m_Words.add("latter");
		m_Words.add("latterly");
		m_Words.add("least");
		m_Words.add("less");
		m_Words.add("lest");
		m_Words.add("let");
		m_Words.add("like");
		m_Words.add("liked");
		m_Words.add("likely");
		m_Words.add("little");
		m_Words.add("ll"); // added to avoid words like you'll,I'll etc.
		m_Words.add("look");
		m_Words.add("looking");
		m_Words.add("looks");
		m_Words.add("ltd");
		m_Words.add("m");
		m_Words.add("mainly");
		m_Words.add("many");
		m_Words.add("may");
		m_Words.add("maybe");
		m_Words.add("me");
		m_Words.add("mean");
		m_Words.add("meanwhile");
		m_Words.add("merely");
		m_Words.add("might");
		m_Words.add("more");
		m_Words.add("moreover");
		m_Words.add("most");
		m_Words.add("mostly");
		m_Words.add("much");
		m_Words.add("must");
		m_Words.add("my");
		m_Words.add("myself");
		m_Words.add("n");
		m_Words.add("name");
		m_Words.add("namely");
		m_Words.add("nd");
		m_Words.add("near");
		m_Words.add("nearly");
		m_Words.add("necessary");
		m_Words.add("need");
		m_Words.add("needs");
		m_Words.add("neither");
		m_Words.add("never");
		m_Words.add("nevertheless");
		m_Words.add("new");
		m_Words.add("next");
		m_Words.add("nine");
		m_Words.add("no");
		m_Words.add("nobody");
		m_Words.add("non");
		m_Words.add("none");
		m_Words.add("noone");
		m_Words.add("nor");
		m_Words.add("normally");
		m_Words.add("not");
		m_Words.add("nothing");
		m_Words.add("novel");
		m_Words.add("now");
		m_Words.add("nowhere");
		m_Words.add("o");
		m_Words.add("obviously");
		m_Words.add("of");
		m_Words.add("off");
		m_Words.add("often");
		m_Words.add("oh");
		m_Words.add("ok");
		m_Words.add("okay");
		m_Words.add("old");
		m_Words.add("on");
		m_Words.add("once");
		m_Words.add("one");
		m_Words.add("ones");
		m_Words.add("only");
		m_Words.add("onto");
		m_Words.add("or");
		m_Words.add("other");
		m_Words.add("others");
		m_Words.add("otherwise");
		m_Words.add("ought");
		m_Words.add("our");
		m_Words.add("ours");
		m_Words.add("ourselves");
		m_Words.add("out");
		m_Words.add("outside");
		m_Words.add("over");
		m_Words.add("overall");
		m_Words.add("own");
		m_Words.add("p");
		m_Words.add("particular");
		m_Words.add("particularly");
		m_Words.add("per");
		m_Words.add("perhaps");
		m_Words.add("placed");
		m_Words.add("please");
		m_Words.add("plus");
		m_Words.add("possible");
		m_Words.add("presumably");
		m_Words.add("probably");
		m_Words.add("provides");
		m_Words.add("q");
		m_Words.add("que");
		m_Words.add("quite");
		m_Words.add("qv");
		m_Words.add("r");
		m_Words.add("rather");
		m_Words.add("rd");
		m_Words.add("re");
		m_Words.add("really");
		m_Words.add("reasonably");
		m_Words.add("regarding");
		m_Words.add("regardless");
		m_Words.add("regards");
		m_Words.add("relatively");
		m_Words.add("respectively");
		m_Words.add("right");
		m_Words.add("s");
		m_Words.add("said");
		m_Words.add("same");
		m_Words.add("saw");
		m_Words.add("say");
		m_Words.add("saying");
		m_Words.add("says");
		m_Words.add("second");
		m_Words.add("secondly");
		m_Words.add("see");
		m_Words.add("seeing");
		m_Words.add("seem");
		m_Words.add("seemed");
		m_Words.add("seeming");
		m_Words.add("seems");
		m_Words.add("seen");
		m_Words.add("self");
		m_Words.add("selves");
		m_Words.add("sensible");
		m_Words.add("sent");
		m_Words.add("serious");
		m_Words.add("seriously");
		m_Words.add("seven");
		m_Words.add("several");
		m_Words.add("shall");
		m_Words.add("she");
		m_Words.add("should");
		m_Words.add("since");
		m_Words.add("six");
		m_Words.add("so");
		m_Words.add("some");
		m_Words.add("somebody");
		m_Words.add("somehow");
		m_Words.add("someone");
		m_Words.add("something");
		m_Words.add("sometime");
		m_Words.add("sometimes");
		m_Words.add("somewhat");
		m_Words.add("somewhere");
		m_Words.add("soon");
		m_Words.add("sorry");
		m_Words.add("specified");
		m_Words.add("specify");
		m_Words.add("specifying");
		m_Words.add("still");
		m_Words.add("sub");
		m_Words.add("such");
		m_Words.add("sup");
		m_Words.add("sure");
		m_Words.add("t");
		m_Words.add("take");
		m_Words.add("taken");
		m_Words.add("tell");
		m_Words.add("tends");
		m_Words.add("th");
		m_Words.add("than");
		m_Words.add("thank");
		m_Words.add("thanks");
		m_Words.add("thanx");
		m_Words.add("that");
		m_Words.add("thats");
		m_Words.add("their");
		m_Words.add("theirs");
		m_Words.add("them");
		m_Words.add("themselves");
		m_Words.add("then");
		m_Words.add("thence");
		m_Words.add("there");
		m_Words.add("thereafter");
		m_Words.add("thereby");
		m_Words.add("therefore");
		m_Words.add("therein");
		m_Words.add("theres");
		m_Words.add("thereupon");
		m_Words.add("these");
		m_Words.add("they");
		m_Words.add("think");
		m_Words.add("third");
		m_Words.add("this");
		m_Words.add("thorough");
		m_Words.add("thoroughly");
		m_Words.add("those");
		m_Words.add("though");
		m_Words.add("three");
		m_Words.add("through");
		m_Words.add("throughout");
		m_Words.add("thru");
		m_Words.add("thus");
		m_Words.add("to");
		m_Words.add("together");
		m_Words.add("too");
		m_Words.add("took");
		m_Words.add("toward");
		m_Words.add("towards");
		m_Words.add("tried");
		m_Words.add("tries");
		m_Words.add("truly");
		m_Words.add("try");
		m_Words.add("trying");
		m_Words.add("twice");
		m_Words.add("two");
		m_Words.add("u");
		m_Words.add("un");
		m_Words.add("under");
		m_Words.add("unfortunately");
		m_Words.add("unless");
		m_Words.add("unlikely");
		m_Words.add("until");
		m_Words.add("unto");
		m_Words.add("useful");
		m_Words.add("uses");
		m_Words.add("using");
		m_Words.add("usually");
		m_Words.add("uucp");
		m_Words.add("v");
		m_Words.add("value");
		m_Words.add("which");
		m_Words.add("while");
		m_Words.add("whither");
		m_Words.add("who");
		m_Words.add("whoever");
		m_Words.add("whole");
		m_Words.add("whom");
		m_Words.add("whose");
		m_Words.add("why");
		m_Words.add("willing");
		m_Words.add("wish");
		m_Words.add("with");
		m_Words.add("within");
		m_Words.add("without");
		m_Words.add("wonder");
		m_Words.add("would");
		m_Words.add("would");
		m_Words.add("x");
		m_Words.add("y");
		m_Words.add("yes");
		m_Words.add("yet");
		m_Words.add("you");
		m_Words.add("your");
		m_Words.add("yours");
		m_Words.add("yourself");
		m_Words.add("yourselves");
		m_Words.add("z");
		m_Words.add("zero");
		// added cause of the HTML and extraction problems
		m_Words.add("nbsp");
		m_Words.add("amp");
		m_Words.add("ouml");
		m_Words.add("uacute");
		m_Words.add("eacute");
		m_Words.add("ordm");
		m_Words.add("agrave");
		m_Words.add("auml");
		m_Words.add("gt");
		m_Words.add("raquo");
		m_Words.add("&uuml");
		// ADDED BASED ON FINDSTOPWORDS WEBTABLES
		m_Words.add("the");
		m_Words.add("of");
		m_Words.add("and");
		m_Words.add("to");
		m_Words.add("in");
		m_Words.add("de");
		m_Words.add("for");
		m_Words.add("com");
		m_Words.add("on");
		m_Words.add("no");
		m_Words.add("gt");
		m_Words.add("is");
		m_Words.add("by");
		m_Words.add("you");
		m_Words.add("with");
		m_Words.add("pm");
		m_Words.add("more");
		m_Words.add("amp");
		m_Words.add("www");
		m_Words.add("or");
		m_Words.add("en");
		m_Words.add("la");
		m_Words.add("mp3");
		m_Words.add("at");
		m_Words.add("http");
		m_Words.add("show");
		m_Words.add("view");
		m_Words.add("new");
		m_Words.add("may");
		m_Words.add("am");
		m_Words.add("from");
		m_Words.add("quot");
		m_Words.add("it");
		m_Words.add("yes");
		m_Words.add("not");
		m_Words.add("lt");
		m_Words.add("this");
		m_Words.add("see");

	}

}
