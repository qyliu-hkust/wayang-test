package org.example.utils;

import org.jgrapht.Graph;
import org.jgrapht.alg.scoring.BetweennessCentrality;
import org.jgrapht.alg.scoring.PageRank;
import org.jgrapht.graph.DefaultWeightedEdge;

import java.util.*;

public class GraphUtils {
    public static List<Pair<Pair<String, String>, Integer>> getNeighbor(List<String> tokens, Collection<String> words, int distance) {
        Map<Pair<String, String>, Integer> wordsPair = new HashMap<>();
        List<Pair<Pair<String, String>, Integer>> results = new ArrayList<>();
        for(int i=0; i < tokens.size(); i++) {
            String w = tokens.get(i);
            if (!words.contains(w)) {
                continue;
            }
            for (int j=i+1; j <=Math.min(tokens.size()-1, i+distance); j++) {
                String wj = tokens.get(j);
                if (!words.contains(wj)) {
                    continue;
                }
                if (wj.equals(w)) {
                    continue;
                }
                Pair<String, String> tempPair = new Pair<>(w, wj);
                if (wordsPair.containsKey(tempPair)) {
                    wordsPair.put(tempPair, wordsPair.get(tempPair) + 1);
                }
                else {
                    wordsPair.put(tempPair, 1);
                }
            }
        }

        for (Pair<String, String> crtWords : wordsPair.keySet()) {
            results.add(new Pair<>(crtWords, wordsPair.get(crtWords)));
        }
        return results;
    }


    public static void insertGraph(Pair<Pair<String, String>, Integer> rawEdge, Graph<String, DefaultWeightedEdge> graph) {
        String word1 = rawEdge.getFirst().getFirst();
        String word2 = rawEdge.getFirst().getSecond();
        graph.addVertex(word1);
        graph.addVertex(word2);
        DefaultWeightedEdge edge = graph.addEdge(word1, word2);
        graph.setEdgeWeight(edge, rawEdge.getSecond());
    }

    public static void insertGraph(List<Pair<Pair<String, String>, Integer>> edges, Graph<String, DefaultWeightedEdge> graph) {
        for (Pair<Pair<String, String>, Integer> edge : edges) {
            insertGraph(edge, graph);
        }
    }


    public static <T> Map<T, Double> computeBetweeness(Graph<T, DefaultWeightedEdge> graph) {
        BetweennessCentrality<T, DefaultWeightedEdge> bc = new BetweennessCentrality<>(graph);
        return bc.getScores();
    }

    public static <T> Map<T, Double> computePageRank(Graph<T, DefaultWeightedEdge> graph) {
        PageRank<T, DefaultWeightedEdge> pagerank = new PageRank<>(graph);
        return pagerank.getScores();
    }
}
