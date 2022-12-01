package org.example.utils;

import java.util.Arrays;
import java.util.List;

public class Document {
    public final long docID;
    public final String text;
    public final List<String> tokens;

    public Document(long docID, String text) {
        this.docID = docID;
        this.text = text;
        this.tokens = Arrays.asList(text.toLowerCase().split(" "));
    }

    public Document(long docID, String text, List<String> tokens) {
        this.docID = docID;
        this.text = text;
        this.tokens = tokens;
    }
}
