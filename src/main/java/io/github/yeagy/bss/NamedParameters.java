package io.github.yeagy.bss;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class NamedParameters {
    private final String unprocessedSql;
    private final String processedSql;
    private final Map<String, List<Integer>> indices;

    private NamedParameters(String unprocessedSql, String processedSql, Map<String, List<Integer>> indices) {
        this.unprocessedSql = unprocessedSql;
        this.processedSql = processedSql;
        this.indices = indices;
    }

    String getUnprocessedSql() {
        return unprocessedSql;
    }

    String getProcessedSql() {
        return processedSql;
    }

    List<Integer> getIndices(String namedParameter) {
        return indices.get(namedParameter);
    }

    /**
     * function copied from http://www.javaworld.com/article/2077706/core-java/named-parameters-for-preparedstatement.html
     * credit to @author adam_crume
     * if this doesn't cut it, consider an ANTLR approach
     *
     * @param sql potential named parameter sql
     * @return named param info. null if none detected.
     */
    static NamedParameters from(String sql) {
        if (!sql.contains("?") && sql.contains(":")) {
            final StringBuilder processedSql = new StringBuilder(sql.length());
            final Map<String, List<Integer>> indices = new HashMap<>();
            int idx = 1;
            boolean inSingleQuote = false;
            boolean inDoubleQuote = false;
            for (int i = 0; i < sql.length(); i++) {
                char c = sql.charAt(i);
                if (inSingleQuote) {
                    if (c == '\'') {
                        inSingleQuote = false;
                    }
                } else if (inDoubleQuote) {
                    if (c == '"') {
                        inDoubleQuote = false;
                    }
                } else {
                    if (c == '\'') {
                        inSingleQuote = true;
                    } else if (c == '"') {
                        inDoubleQuote = true;
                    } else if (c == ':' && i + 1 < sql.length() && Character.isJavaIdentifierStart(sql.charAt(i + 1))) {
                        int j = i + 2;
                        while (j < sql.length() && Character.isJavaIdentifierPart(sql.charAt(j))) {
                            j++;
                        }
                        final String name = sql.substring(i + 1, j);
                        c = '?';
                        i += name.length();
                        multimapPut(indices, name, idx++);
                    }
                }
                processedSql.append(c);
            }
            if (!indices.isEmpty()) {
                return new NamedParameters(sql, processedSql.toString(), indices);
            }
        }
        return null;
    }

    private static void multimapPut(Map<String, List<Integer>> map, String s, Integer i) {
        List<Integer> list = map.get(s);
        if (list == null) {
            list = new ArrayList<>();
            map.put(s, list);
        }
        list.add(i);
    }
}
