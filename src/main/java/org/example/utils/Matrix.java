package org.example.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Matrix<R, C, V> {
    private final List<R> rowMapping;
    private final List<C> columnMapping;
    private final List<List<V>> values;
    private final int rowCnt;
    private final int columnCnt;

    public Matrix(List<R> rowMapping, List<C> columnMapping, V initValue) {
        this.rowMapping = rowMapping;
        this.columnMapping = columnMapping;
        this.rowCnt = rowMapping.size();
        this.columnCnt = columnMapping.size();
        this.values = new ArrayList<>(rowCnt);

        for (int i=0; i<rowCnt; ++i) {
            List<V> temp = new ArrayList<>(columnCnt);
            for (int j=0; j<columnCnt; ++j) {
                temp.add(initValue);
            }
            values.add(temp);
        }
    }

    public List<R> getRowMapping() {
        return rowMapping;
    }

    public List<C> getColumnMapping() {
        return columnMapping;
    }

    public int getRowCnt() {
        return rowCnt;
    }

    public int getColumnCnt() {
        return columnCnt;
    }

    public V getVal(int i, int j) {
        return values.get(i).get(j);
    }

    public void setVal(int i, int j, V val) {
        values.get(i).set(j, val);
    }

    public List<List<V>> getValues() {
        return values;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("--\t");
        builder.append(columnMapping.stream().map(Object::toString).collect(Collectors.joining("\t")));
        builder.append(System.lineSeparator());
        for (int i=0; i<rowCnt; ++i) {
            List<String> collect = values.get(i).stream().map(Object::toString).collect(Collectors.toList());
            builder.append(rowMapping.get(i).toString());
            builder.append("\t");
            builder.append(String.join("\t", collect));
            builder.append(System.lineSeparator());
        }
        return builder.toString();
    }
}
