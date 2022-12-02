package org.example.query3;

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


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class QueryThree {
    private static final String URL = "jdbc:postgresql://localhost/test";
    private static final String USER = "";
    private static final String PASSWD = "";

    public static void main(String[] args) throws SQLException {
        int rows = 50;

        Connection conn = DriverManager.getConnection(URL, USER, PASSWD);

        List<String> keywords = Arrays.asList("corona", "covid", "pandemic", "vaccine");

    }
}
