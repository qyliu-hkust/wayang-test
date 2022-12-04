package org.example.utils;
import java.io.*;
import java.util.*;


public class AutoPhraseUtil {
    private final String dataPath = "data/EN/awesome.txt";
    private final String modelPath = "models/AWESOME";
    private final String workspace = "/users/Xiuwen/AutoPhrase/";
    private final int num;
    private final int min_sup;
    private final List<String> documents;


    public AutoPhraseUtil(List<String> docs, int min_sup, int num) {
        this.min_sup = min_sup;
        this.num = num;
        this.documents = docs;
    }

    public AutoPhraseUtil(List<String> docs, int num) {
        this.min_sup = docs.size() / 500;
        this.num = num;
        this.documents = docs;
    }

    public List<String> callScript() {
        List<String> keywords = new ArrayList<>();
        try {
            System.out.println("load data from database");
            // todo: store documents
            storeText(this.documents, this.workspace + this.dataPath, false);
            String scriptPath = "auto_phrase.sh";
            String cmd = "bash  " + scriptPath + " " + this.dataPath + " " + this.min_sup;
            File dir = new File(this.workspace);
            Process process = Runtime.getRuntime().exec(cmd, null, dir);
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                System.out.println(line);
            }
            input.close();
            keywords = getSingleKeywords(this.num);
            Runtime.getRuntime().exec("rm " + this.dataPath, null, dir);
            System.out.println("autophrase finished");
            System.out.println("keywords: " + keywords.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return keywords;
    }

    private List<String> getSingleKeywords(int num) throws IOException {
        String resultLoc = this.workspace + this.modelPath + "/AutoPhrase_single-word.txt";
        BufferedReader objReader = new BufferedReader(new FileReader(resultLoc));
        String crtLine;
        List<Float> score = new ArrayList<>();
        List<String> phrase = new ArrayList<>();
        int count = 0;
        while ((crtLine = objReader.readLine()) != null) {
            String[] data = crtLine.split("\t");
            float s = Float.parseFloat(data[0]);
            if (count >= num) {
                break;
            }
            count += 1;
            score.add(s);
            phrase.add(data[1]);
        }
        return phrase;
    }

    private void storeText(List<String> docs,  String dataPath, boolean append) throws IOException {
        Writer output = new BufferedWriter(new FileWriter(dataPath, append));

        for (String doc: docs) {
            output.write(doc + System.lineSeparator());
        }
        output.close();
    }
}
