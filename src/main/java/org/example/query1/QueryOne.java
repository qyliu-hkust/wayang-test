package org.example.query1;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.sqlite3.Sqlite3;
import org.example.utils.Document;
import org.example.utils.GraphUtils;
import org.example.utils.Pair;
import org.example.utils.TextUtils;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import java.io.IOException;
import java.util.*;

//  Query 1 Implementation
//  use newsDB as polystore;
//  create analysis awardanalysis as (
//      abstracts := executeSQL("Awesome", "select abstract from sbir_award_data where abstract is not null  limit 10000");
//      docs := tokenize(abstracts.abstract, stopwords="C:\Users\xiuwen\IdeaProjects\awesome-new-version\stopwords.txt");
//      keywords := keyphraseMining(docs, 2000);
//      wordsPair := buildWordNeighborGraph(docs, words=keywords);
//      graph := ConstructGraphFromRelation(wordsPair, (:Word {value: $wordsPair.word1}) -[:Cooccur{count: $wordsPair.count}]->(:Word{value: $wordsPair.word2}));
//      betweenness := betweenness(graph);
//      pagerank := pageRank(graph);
//  );

public class QueryOne {
    public static void main(String[] args) throws IOException {
        String dataPath = "../data/sbir_award_data.csv";
        int limit = 50000;
        int maxKeywordSize = 2000;
        int distance = 100;

        System.out.println(String.format("#Document: %d, maxKeywordSize: %d, distance: %d",
                limit, maxKeywordSize, distance));

        long start = System.currentTimeMillis();

        List<Document> documents = TextUtils.createDocsFromCSV(dataPath, limit);

        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Sqlite3.conversionPlugin());

//        JavaPlanBuilder planBuilder1 = new JavaPlanBuilder(wayangContext)
//                .withJobName("Betweeness Phase 1: collect tokens and extract keywords")
//                .withUdfJarOf(QueryOne.class);
//
//        Collection<List<String>> collect1 = planBuilder1.loadCollection(documents)
//                .map(doc -> doc.tokens)
//                .withName("document to tokens")
//
//                .reduce((docTokens1, docTokens2) -> {
//                    List<String> combined = new ArrayList<>(docTokens1.size() + docTokens2.size());
//                    combined.addAll(docTokens1);
//                    combined.addAll(docTokens2);
//                    return combined;
//                })
//                .withName("merge all tokens")
//                .collect();

//        assert collect1.size() == 1;

//        List<String> allTokens = collect1.iterator().next();
//        String longDoc = String.join(" ", allTokens);
//        List<String> keywords = TextUtils.executePhraseMining(longDoc, maxKeywordSize);

        List<String> keywords = TextUtils.executeAutoPhrase(documents, maxKeywordSize);
        Set<String> keywordSet = new HashSet<>(keywords);

        JavaPlanBuilder planBuilder2 = new JavaPlanBuilder(wayangContext)
                .withJobName("Betweeness Phase 2: build word neighbour graph")
                .withUdfJarOf(QueryOne.class);

        Collection<Pair<Pair<String, String>, Integer>> collect2 = planBuilder2.loadCollection(documents)
                .map(doc -> doc.tokens)
                .withName("document to tokens")

                .flatMap(tokens -> GraphUtils.getNeighbor(tokens, keywordSet, distance))
                .withName("get graph elements")

                .reduceByKey(Pair::getFirst, (p1, p2) -> new Pair<>(p1.getFirst(), p1.getSecond() + p2.getSecond()))
                .withName("merge graph elements")
                .collect();

        Graph<String, DefaultWeightedEdge> graph = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
        for (Pair<Pair<String, String>, Integer> rawEdge : collect2) {
            GraphUtils.insertGraph(rawEdge, graph);
        }

        System.out.println("edge size: " + graph.edgeSet().size());

        System.out.println("Compute betweeness.");

        Map<String, Double> betweeness = GraphUtils.computeBetweeness(graph);

        long end = System.currentTimeMillis();
        System.out.println("Total time (ms): " + (end - start));

        int cnt = 0;
        for (String key : betweeness.keySet()) {
            if (cnt < 10) {
                System.out.println(key + " " + betweeness.get(key));
                cnt++;
            }
        }
    }
}
