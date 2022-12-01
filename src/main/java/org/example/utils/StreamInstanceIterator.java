package org.example.utils;

import cc.mallet.types.Instance;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.stream.Stream;

public class StreamInstanceIterator implements Iterator<Instance> {
    private Iterator<Document> subIterator;
    private Object target;
    private int index;

    public StreamInstanceIterator(Stream<Document> documentStream) {
        this.subIterator = documentStream.iterator();
        this.target = null;
        this.index = 0;
    }

    @Override
    public boolean hasNext() {
        return subIterator.hasNext();
    }

    @Override
    public void remove() {
        subIterator.remove();
    }

    @Override
    public Instance next() {
        try {
            URI uri = new URI("document:" + index++);
            return new Instance(subIterator.next().text, target, uri, null);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        return null;
    }
}
