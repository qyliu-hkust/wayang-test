package org.example.query3;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.Directory;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.sqlite3.Sqlite3;

import org.example.utils.*;


//    Query 3
//    use newsDB as polystore;
//    create analysis politician as (
//        keywords := ["corona", "covid", "pandemic", "vaccine"];
//        temp := keywords.#map(i => stringReplace("text-field: $", i));
//        t := stringJoin(" OR ", temp);
//        doc<text-field:String> := executeSOLR("allnews", """q= $t & rows=50""");
//        namedentity := NER(doc.text-field);
//        user := executeSQL("News", "select distinct t.name as name, t.twittername as twittername from twitterhandle t, $namedentity e where LOWER(e.name)=LOWER(t.name)");
//        userNameList := toList(user.name);
//        userNameP := userNameList.#map(i => stringReplace("t.text contains '$' ", i));
//        predicate := stringJoin(" OR ", userNameP);
//        users<name:String> := executeCypher("tweetG", "match (u:User)-[:mention]-(n:User) where n.userName in $user.twittername return u.userName as name");
//        tweet<t:String> :=  executeCypher("tweetG", "match (t:Tweet) where $predicate return t.text as t");
//    );


public class QueryThree {

    private static final String URL = "jdbc:postgresql://awesome-hw.sdsc.edu/";
    private static final String USER_NAME = "postgres";
    private static final String PASSWD = "Sdsc2018#";
    private static final String DB_NAME = "postgres";

    private static List<Pair<Long, String>> loadUserTable(String path, int size) throws IOException {
        List<Pair<Long, String>> data = new ArrayList<>();
        File csvFile = new File(path + "Twitter_user_" + size + ".csv");
        CSVParser parser = CSVFormat.DEFAULT.parse(new FileReader(csvFile));
        for (CSVRecord record : parser) {
            long id = Long.parseLong(record.get(0));
            String name = record.get(1);
            data.add(new Pair<Long, String>(id, name));
        }
        return data;
    }

    private static List<Pair<Long, Long>> loadUserTweetTable(String path, int size) throws IOException {
        List<Pair<Long, Long>> data = new ArrayList<>();
        File csvFile = new File(path + "Twitter_user_tweet_" + size + ".csv");
        CSVParser parser = CSVFormat.DEFAULT.parse(new FileReader(csvFile));
        for (CSVRecord record : parser) {
            long userID = Long.parseLong(record.get(0));
            long tweetID = Long.parseLong(record.get(1));
            
            data.add(new Pair<Long, Long>(userID, tweetID));
        }
        return data;
    }

    private static List<Pair<Long, Long>> loadUserMentionTable(String path, int size) throws IOException {
        List<Pair<Long, Long>> data = new ArrayList<>();
        File csvFile = new File(path + "Twitter_user_user_" + size + ".csv");
        CSVParser parser = CSVFormat.DEFAULT.parse(new FileReader(csvFile));
        for (CSVRecord record : parser) {
            long userID1 = Long.parseLong(record.get(0));
            long userID2 = Long.parseLong(record.get(1));
            
            data.add(new Pair<Long, Long>(userID1, userID2));
        }
        return data;
    }

    private static List<Pair<Long, String>> loadTweetTable(int size) {
        List<Pair<Long, String>> data = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(URL, USER_NAME, PASSWD);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select * from xw_neo4j_tweet" + size)) {
            while (rs.next()) {
                long first = rs.getLong(1);
                String second = rs.getString(2);
                data.add(new Pair<>(first, second));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return data;
    }

    private static List<String> query1(int size, List<String> twitterNames) {
        String sql = "select uu.userid1, uu.userid2, u.username" +
                     "from xw_neo4j_user_user%d as uu, xw_neo4j_user%d as u" +
                     "where uu.userid2 = u.userid and u.username in ('lianystr', 'aaas')";
        sql = String.format(sql, size, size);
        System.out.println(sql);

        List<String> names = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(URL, USER_NAME, PASSWD);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String name = rs.getString(3);
                names.add(name);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return names;
    }

    public static void main(String[] args) throws IOException, ParseException {
        int rows = 5000;
        int graphSize = 100000;

        if (args.length > 0) {
            rows = Integer.parseInt(args[0]);
            graphSize = Integer.parseInt(args[1]);
        }
        
        List<String> keywords = Arrays.asList("corona", "covid", "pandemic", "vaccine");
        List<Document> documents = TextUtils.createDocsFromCSV("../data/newssolr.csv", Integer.MAX_VALUE, 1, 0, false);
        Analyzer analyzer = new StandardAnalyzer();
        Directory indexDirectory = TextUtils.createTextIndex(documents, analyzer);
        

        long start = System.currentTimeMillis();

        List<Document> queryResults = TextUtils.searchTextIndex(indexDirectory, keywords, rows, analyzer);

        Map<String, Set<String>> nerResults = TextUtils.executeNER(queryResults);

        List<Pair<String, String>> senatorHandles = TextUtils.loadPairsFromCSV("../data/senatorhandle.csv", 0, 1, false);
        
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Sqlite3.conversionPlugin());

        JavaPlanBuilder planBuilder1 = new JavaPlanBuilder(wayangContext)
                .withJobName("get named entities")
                .withUdfJarOf(QueryThree.class);
        
        Collection<String> namedEntities = planBuilder1.loadCollection(nerResults.get("PERSON"))
                .map(name -> name.toLowerCase())
                .collect();
        Set<String> namedEntitySet = new HashSet<>(namedEntities);

        JavaPlanBuilder planBuilder2 = new JavaPlanBuilder(wayangContext)
                .withJobName("match twitter handlers")
                .withUdfJarOf(QueryThree.class);

        Collection<Pair<String, String>> matchedEntities = planBuilder2.loadCollection(senatorHandles)
                .filter(s -> namedEntitySet.contains(s.getFirst().toLowerCase()))
                .collect();
        
        System.out.println("#matched entities: " + matchedEntities);
        
        List<Pair<Long, String>> tweetTable = loadTweetTable(graphSize);

        // query1(graphSize, matchedEntities.stream().map(e -> e.getSecond()).collect(Collectors.toList()));

        JavaPlanBuilder planBuilder3 = new JavaPlanBuilder(wayangContext)
                .withJobName("match twitter handlers")
                .withUdfJarOf(QueryThree.class);

        Collection<String> tweets = planBuilder3.loadCollection(tweetTable)
                .map(t -> t.getSecond())
                .filter(text -> {
                    boolean flag = false;
                    for (Pair<String, String> e : matchedEntities) {
                        if (text.indexOf(e.getFirst()) != -1) {
                            flag = true;
                            break;
                        } 
                    }
                    return flag;
                })
                .collect();

        long end = System.currentTimeMillis();
        System.out.println("total time: " + (end - start));
    }
}
