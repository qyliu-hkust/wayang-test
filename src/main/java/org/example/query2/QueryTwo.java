package org.example.query2;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.sqlite3.Sqlite3;

import org.example.utils.*;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// Query 2 Implementation
// use newsDB as polystore;
// create analysis newstopicanalysis as (
// src :=  " http://www.chicagotribune.com/";
// rawNews := executeSQL("News", "select id as newsid, news as newstext from usnewspaper where src = $src order by id limit 1000");
// processedNews := tokenize(rawNews.newstext, docid=rawNews.newsid, stopwords="C:\Users\xiuwen\IdeaProjects\awesome-new-version\stopwords.txt");
// numberTopic := 10;
// DTM, WTM := lda(processedNews, docid=true, topic=numberTopic);
// topicID := [range(0, numberTopic, 1)];
// wtmPerTopic := topicID.#map(i => WTM where getValue(_:Row, i) > 0.00);
// wordsPerTopic := wtmPerTopic.#map(i => rowNames(i));
// wordsOfInterest := union(wordsPerTopic);
// G := buildWordNeighborGraph(processedNews, maxDistance=5, splitter=".", words=wordsOfInterest);
// relationPerTopic := wordsPerTopic.#map(words => executeSQL("News", "select word1 as n, word2 as m, count from $G where word1 in ($words) and word2 in ($words)"));
// graphPerTopic := relationPerTopic.#map(r => ConstructGraphFromRelation(r, (:Word {value: $r.word1}) -[:Cooccur{count: $r.count}]->(:Word{value: $r.m})));
// scores := graphPerTopic.#map(g => pageRank(g, topk=true, num=20));
// );

public class QueryTwo {
    public static void main(String[] args) throws IOException {
        String newsPath = "../newspg.csv";
        String src = " http://www.chicagotribune.com/";
        int limit = 10000;
        int numWords = 10000;
        int numTopics = 10;
        int maxDistance = 5;
        int pgTopK = 20;

        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Sqlite3.conversionPlugin());

        long start = System.currentTimeMillis();

        JavaPlanBuilder planBuilder1 = new JavaPlanBuilder(wayangContext)
                .withJobName("Phase 1: execute LDA")
                .withUdfJarOf(QueryTwo.class);

        List<Document> documents = TextUtils.createDocsFromCSV(newsPath, limit, 1, 0, 8, a->a.equals(src));
        Pair<Matrix<Integer, Integer, Double>, Matrix<String, Integer, Double>> LDAResult = TextUtils.executeLDA(documents, numTopics, numWords);
        Matrix<String, Integer, Double> rawWTM = LDAResult.getSecond();
        List<Integer> topicIDs = IntStream.rangeClosed(0, numTopics-1).boxed().collect(Collectors.toList());

        List<List<Double>> rawWTMRows = rawWTM.getValues();
        List<String> rowNames = rawWTM.getRowMapping();

        Collection<List<String>> wordsPerTopic = planBuilder1.loadCollection(topicIDs)
                .withName("load topic IDs")

                .map(topicID -> {
                    List<Integer> wordIndices = new ArrayList<>();
                    for (int i = 0; i < rawWTMRows.size(); ++i) {
                        if (rawWTMRows.get(i).get(topicID) > 0.0) {
                            wordIndices.add(i);
                        }
                    }
                    return wordIndices;
                })
                .withName("find all words (index) of interest")

                .map(wordIndices -> {
                    List<String> words = new ArrayList<>();
                    for (int idx : wordIndices) {
                        words.add(rowNames.get(idx));
                    }
                    return words;
                })
                .withName("word index to word string")

                .collect();

        Set<String> wordSet = new HashSet<>();
        for (List<String> words : wordsPerTopic) {
            wordSet.addAll(words);
        }

        List<String> wordsOfInterest = new ArrayList<>(wordSet);

        JavaPlanBuilder planBuilder2 = new JavaPlanBuilder(wayangContext)
                .withJobName("Phase 2: build graph and run pagerank")
                .withUdfJarOf(QueryTwo.class);

        Collection<Pair<Pair<String, String>, Integer>> graphElements = planBuilder2.loadCollection(documents)
                .map(doc -> doc.tokens)
                .withName("document to tokens")

                .flatMap(tokens -> GraphUtils.getNeighbor(tokens, wordsOfInterest, maxDistance))
                .withName("get graph elements")

                .reduceByKey(Pair::getFirst, (p1, p2) -> new Pair<>(p1.getFirst(), p1.getSecond() + p2.getSecond()))
                .withName("merge graph elements")
                .collect();

        Graph<String, DefaultWeightedEdge> graph = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
        for (Pair<Pair<String, String>, Integer> rawEdge : graphElements) {
            GraphUtils.insertGraph(rawEdge, graph);
        }

        JavaPlanBuilder planBuilder3 = new JavaPlanBuilder(wayangContext)
                .withJobName("Phase 3: build graph for each topic")
                .withUdfJarOf(QueryTwo.class);

        Collection<List<Pair<String, Double>>> topKWordScores = planBuilder3.loadCollection(wordsPerTopic)
                .map(keywords -> (Set<String>) new HashSet<>(keywords))
                .withName("keywords list to set")

                .map(keywordSet -> {
                    List<Pair<Pair<String, String>, Integer>> tuples = new ArrayList<>();
                    for (DefaultWeightedEdge e : graph.edgeSet()) {
                        String word1 = graph.getEdgeSource(e);
                        String word2 = graph.getEdgeTarget(e);
                        int cnt = (int) graph.getEdgeWeight(e);

                        if (keywordSet.contains(word1) && keywordSet.contains(word2)) {
                            tuples.add(new Pair<>(new Pair<>(word1, word2), cnt));
                        }
                    }
                    return tuples;
                })
                .withName("executeSQL(News, select word1 as n, word2 as m, count from $G where word1 in ($words) and word2 in ($words))")

                .map(tuples -> {
                    Graph<String, DefaultWeightedEdge> topicGraph = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
                    GraphUtils.insertGraph(tuples, topicGraph);
                    return topicGraph;
                })
                .withName("construct graph from tuples for each topic")

                .map(GraphUtils::computePageRank)
                .withName("execute pagerank for each graph")

                .map(pgScores -> {
                    List<Map.Entry<String, Double>> sorted = pgScores.entrySet().stream()
                            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                            .limit(pgTopK)
                            .collect(Collectors.toList());
                    List<Pair<String, Double>> wordScores = new ArrayList<>(sorted.size());
                    for (Map.Entry<String, Double> entry : sorted) {
                        wordScores.add(new Pair<>(entry.getKey(), entry.getValue()));
                    }
                    return wordScores;
                })
                .withName("sort words by pagerank score and return top-k words")
                .collect();

        long end = System.currentTimeMillis();
        System.out.println("Total time (ms): " + (end - start));

        topKWordScores.forEach(System.out::println);
    }
}
