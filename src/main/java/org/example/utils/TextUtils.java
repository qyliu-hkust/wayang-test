package org.example.utils;

import cc.mallet.pipe.*;
import cc.mallet.topics.ParallelTopicModel;
import cc.mallet.types.Alphabet;
import cc.mallet.types.IDSorter;
import cc.mallet.types.InstanceList;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.util.Triple;
import io.github.crew102.rapidrake.RakeAlgorithm;
import io.github.crew102.rapidrake.model.RakeParams;
import io.github.crew102.rapidrake.model.Result;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TextUtils {
    public static List<Document> createDocsFromCSV(String path, int limit) throws IOException {
        List<Document> documents = new ArrayList<>();
        File csvFile = new File(path);
        CSVParser parser = CSVFormat.DEFAULT.parse(new FileReader(csvFile));
        int cnt = 0;
        for (CSVRecord record : parser) {
            String text = record.get(0);
            long id = Long.parseLong(record.get(1));
            documents.add(new Document(id, text));
            cnt ++;
            if (cnt >= limit) {
                break;
            }
        }
        return documents;
    }

    public static List<Document> createDocsFromCSV(String path, int limit, int idPosition, int textPostition, boolean isHeader)
        throws IOException {
        List<Document> documents = new ArrayList<>();
        File csvFile = new File(path);
        CSVParser parser;
        if (isHeader) {
            parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(csvFile));
        } else {
            parser = CSVFormat.DEFAULT.parse(new FileReader(csvFile));
        }
        int cnt = 0;
        for (CSVRecord record : parser) {
            long id = Long.parseLong(record.get(idPosition));
            String text = record.get(textPostition);
            documents.add(new Document(id, text));
            cnt ++;
            if (cnt >= limit) {
                break;
            }
        }
        return documents;
    }

    public static List<Document> createDocsFromCSV(String path, int limit, int idPosition, int textPostition, int filterFieldPosition, Predicate<String> filterPred)
        throws IOException {
        List<Document> documents = new ArrayList<>();
        File csvFile = new File(path);
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(csvFile));
        int cnt = 0;
        for (CSVRecord record : parser) {
            long id = Long.parseLong(record.get(idPosition));
            String text = record.get(textPostition);
            String filter = record.get(filterFieldPosition);
            if (filterPred.test(filter)) {
                documents.add(new Document(id, text));
                cnt ++;
                if (cnt >= limit) {
                    break;
                }
            }
        }
        return documents;
    }


    public static List<Pair<String, String>> loadPairsFromCSV(String path, int firstPosition, int secondPosition, boolean isHeader) 
        throws IOException {
        List<Pair<String, String>> pairs = new ArrayList<>();
        File csvFile = new File(path);
        CSVParser parser;
        if (isHeader) {
            parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(csvFile));
        } else {
            parser = CSVFormat.DEFAULT.parse(new FileReader(csvFile));
        }
        for (CSVRecord record : parser) {
            String first = record.get(firstPosition);
            String second = record.get(secondPosition);
            pairs.add(new Pair<String, String>(first, second));
        }
        return pairs;
    }


    public static Set<String> loadStopWordsFromFile(String path) {
        Set<String> stopWords = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            reader.lines().forEach(l -> stopWords.add(l.toLowerCase()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stopWords;
    }


    public static List<String> tokenize(String text, Set<String> stopWords) {
        return null;
    }


    private final static String delims = "[-,.?():;\"!/]";
    private final static String posUrl = "../model-bin/en-pos-maxent.bin";
    private final static String sentUrl = "../model-bin/en-sent.bin";


    public static List<String> executePhraseMining(String text, int maxKeywordSize) throws IOException {
        Set<String> keywordSet = new HashSet<>();

        String[] stopPOS = {"VBD"};
        RakeParams params = new RakeParams(new String[0], stopPOS, 0, true, delims);
        RakeAlgorithm algo = new RakeAlgorithm(params, posUrl, sentUrl);
        Result result = algo.rake(text);
        String[] fullKeywords = result.getFullKeywords();

        for (String k : fullKeywords) {
            if (!k.contains(" ")) {
                keywordSet.add(k);
            }
            if (keywordSet.size() >= maxKeywordSize) {
                break;
            }
        }

        List<String> keywords = new ArrayList<>(keywordSet.size());
        keywords.addAll(keywordSet);

        return keywords;
    }

    public static List<String> executePhraseMining(List<Document> documents, int maxKeywordSize) throws IOException {
        List<String> textList = new ArrayList<>();
        for (Document document : documents) {
            textList.add(String.join(" ", document.tokens));
        }
        String longDoc = String.join(" ", textList);
        return executePhraseMining(longDoc, maxKeywordSize);
    }

    public static List<String> executeAutoPhrase(List<Document> documents, int maxKeywordSize) {
        List<String> textList = new ArrayList<>();
        for (Document doc : documents) {
            textList.add(String.join(" ", doc.tokens));
        }
        AutoPhraseUtil util = new AutoPhraseUtil(textList, maxKeywordSize);
        return util.callScript();
    }

    private static final int LDA_ITERATION_NUM = 1000;

    public static Pair<Matrix<Integer, Integer, Double>, Matrix<String, Integer, Double>> executeLDA(List<Document> documents, int maxTopicNum, int maxWordNum)
            throws IOException {
        Pattern pattern = Pattern.compile("\\p{L}[\\p{L}\\p{P}]+\\p{L}");
        List<Pipe> pipeList = Arrays.asList(new CharSequenceLowercase(),
                new CharSequence2TokenSequence(pattern),
                new TokenSequenceRemoveStopwords(new File("./stopwords.txt"), "UTF-8", false, false, false),
                new TokenSequence2FeatureSequence()
        );
        InstanceList instanceList = new InstanceList(new SerialPipes(pipeList));
        instanceList.addThruPipe(new StreamInstanceIterator(documents.stream()));

        ParallelTopicModel model = new ParallelTopicModel(maxTopicNum, 0.1, 0.01);
        model.setRandomSeed(2);
        model.addInstances(instanceList);
        model.setNumThreads(Runtime.getRuntime().availableProcessors());
        model.setNumIterations(LDA_ITERATION_NUM);
        model.setOptimizeInterval(0);

        model.estimate();

        // init document-topic matrix
        List<Integer> topicIDs = IntStream.rangeClosed(0, maxTopicNum - 1).boxed().collect(Collectors.toList());
        List<Integer> documentIDs = IntStream.rangeClosed(0, documents.size() - 1).boxed().collect(Collectors.toList());
        Matrix<Integer, Integer, Double> docTopicMatrix = new Matrix<>(documentIDs, topicIDs, 0.0);

        // fill document-topic matrix
        for (int i=0; i<documents.size()-1; ++i) {
            double[] topicProbs = model.getTopicProbabilities(i);
            for (int j=0; j<topicProbs.length; ++j) {
                docTopicMatrix.setVal(i, j, topicProbs[j]);
            }
        }

        // init word-topic matrix
        Alphabet alphabet = instanceList.getDataAlphabet();
        List<String> words = new ArrayList<>(alphabet.size());
        for (int i=0; i<alphabet.size(); ++i) {
            words.add((String) alphabet.lookupObject(i));
        }
        Matrix<String, Integer, Double> wordTopicMatrix = new Matrix<>(words, topicIDs, 0.0);

        // fill word-topic matrix
        List<TreeSet<IDSorter>> sortedWords = model.getSortedWords();
        for (int i=0; i<maxTopicNum; ++i) {
            int cnt = 0;
            for (IDSorter s : sortedWords.get(i)) {
                if (cnt >= maxWordNum) {
                    break;
                }
                wordTopicMatrix.setVal(s.getID(), i, 1.0);
                cnt ++;
            }
        }

        return new Pair<>(docTopicMatrix, wordTopicMatrix);
    }

    public static Map<String, Set<String>> executeNER(List<Document> documents) {
        String modelPath = "./classifiers/english.all.3class.distsim.crf.ser.gz";
        Map<String, Set<String>> nerResults = new ConcurrentHashMap<>();
        try {
            AbstractSequenceClassifier<CoreLabel> classifier = CRFClassifier.getClassifier(modelPath);
            documents.parallelStream()
                .forEach(doc -> {
                    List<Triple<String, Integer, Integer>> predict = classifier.classifyToCharacterOffsets(doc.text);
                    for (Triple<String, Integer, Integer> label : predict) {
                        String type = label.first();
                        String entity = doc.text.substring(label.second(), label.third());
                        if (nerResults.containsKey(type)) {
                            nerResults.get(type).add(entity);
                        } else {
                            Set<String> entities = new HashSet<>();
                            entities.add(entity);
                            nerResults.put(type, entities);
                        }
                    }
                });
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return nerResults;
    }

    // return the index directory
    public static Directory createTextIndex(List<Document> documents, Analyzer analyzer) throws IOException {
        // Path indexPath = Files.createTempDirectory("tempIndex");
        Path indexPath = Paths.get("./tempIndex");
        if (!Files.exists(indexPath)) {
            Files.createDirectory(indexPath);
            Directory directory = FSDirectory.open(indexPath);
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            IndexWriter iwriter = new IndexWriter(directory, config);
            for (Document myDoc : documents) {
                org.apache.lucene.document.Document indexDoc = new org.apache.lucene.document.Document();
                indexDoc.add(new Field("text-field", myDoc.text, TextField.TYPE_STORED));
                indexDoc.add(new Field("id", String.valueOf(myDoc.docID), TextField.TYPE_STORED));
                iwriter.addDocument(indexDoc);
            }
            iwriter.close();
            return directory;
        } else {
            return FSDirectory.open(indexPath);
        }
    }

    public static List<Document> searchTextIndex(Directory indexDir, List<String> keywords, int n, Analyzer analyzer) throws IOException, ParseException {
        DirectoryReader iReader = DirectoryReader.open(indexDir);
        IndexSearcher iSearcher = new IndexSearcher(iReader);
        QueryParser parser = new QueryParser("text-field", analyzer);
        Query q = parser.parse(String.join(" OR ", keywords));
        TopDocs topDocs = iSearcher.search(q, n);
        
        List<Document> result = new ArrayList<>(topDocs.scoreDocs.length);
        for (ScoreDoc sd : topDocs.scoreDocs) {
            String text = iSearcher.doc(sd.doc).get("text-field");
            long id = Long.parseLong(iSearcher.doc(sd.doc).get("id"));
            result.add(new Document(id, text));
        }



        return result;
    }


    public static void main(String[] args) {
        try {
            // List<Document> documents= createDocsFromCSV("../data/sbir_award_data.csv", Integer.MAX_VALUE);
            // long start = System.currentTimeMillis();
            // Map<String, Set<String>> ner = executeNER(documents);
            // long end = System.currentTimeMillis();
            // System.out.println(end - start);
            
            List<Document> documents = TextUtils.createDocsFromCSV("../data/newssolr.csv", Integer.MAX_VALUE, 1, 0, false);
            Analyzer analyzer = new StandardAnalyzer();
            Directory indexDirectory = TextUtils.createTextIndex(documents, analyzer);
            List<String> keywords = Arrays.asList("corona", "covid", "pandemic", "vaccine");
            List<Document> queryResults = TextUtils.searchTextIndex(indexDirectory, keywords, 10, analyzer);
            queryResults.forEach(doc -> System.out.println(doc.text));
            
            // for (Document doc : searchTextIndex(dir, keywords, 100, analyzer)) {
            //     System.out.println(doc.docID);
            //     System.out.println(doc.text);
            // }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
