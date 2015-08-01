/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.query.parser;

/**
 * Parser of query parts. This parser is aware of parenthesis symbols '(' and
 * ')' and only yields tokens that have balanced parentheses. Delimitors are
 * configurable.
 */
public final class QueryPartParser {

    private final QueryPartProcessor _processor;
    private final String _clause;
    private final String[] _ItemDelims;

    public QueryPartParser(QueryPartProcessor processor, String clause, String... itemDelims) {
        if (clause == null) {
            throw new IllegalArgumentException("Clause cannot be null");
        }
        if (itemDelims == null || itemDelims.length == 0) {
            throw new IllegalArgumentException("Item delimitors cannot be null or empty");
        }
        _processor = processor;
        _clause = clause;
        _ItemDelims = itemDelims;
    }

    public void parse() {
        if (_clause.isEmpty()) {
            return;
        }

        int parenthesisCount = 0;
        int offset = 0;
        boolean singleOuterParenthesis = _clause.charAt(0) == '(' && _clause.charAt(_clause.length() - 1) == ')';

        String previousDelim = null;
        DelimOccurrence nextDelimOccurrence = getNextDelim(0);
        if (nextDelimOccurrence != null) {
            for (int i = 0; i < _clause.length(); i++) {
                char c = _clause.charAt(i);
                if (c == '(') {
                    parenthesisCount++;
                } else if (c == ')') {
                    parenthesisCount--;
                    if (singleOuterParenthesis && parenthesisCount == 0 && i != _clause.length() - 1) {
                        singleOuterParenthesis = false;
                    }
                }
                if (i == nextDelimOccurrence.index) {
                    if (parenthesisCount == 0) {
                        // token bounds has been identified
                        String itemToken = _clause.substring(offset, i);
                        parseItem(previousDelim, itemToken);
                        offset = i + nextDelimOccurrence.delim.length();
                        previousDelim = nextDelimOccurrence.delim;
                    }
                    nextDelimOccurrence = getNextDelim(nextDelimOccurrence.index + 1);
                    if (nextDelimOccurrence == null) {
                        break;
                    }
                }
            }
        }

        if (singleOuterParenthesis) {
            String newClause = _clause.substring(1, _clause.length() - 1);
            // re-run based on new clause
            QueryPartParser newParser = new QueryPartParser(_processor, newClause, _ItemDelims);
            newParser.parse();
            return;
        }

        // last token will occur outside loop
        if (offset != _clause.length()) {
            final String token = _clause.substring(offset);
            parseItem(previousDelim, token);
        }
    }

    private static class DelimOccurrence {
        public int index;
        public String delim;
    }

    private DelimOccurrence getNextDelim(int offset) {
        DelimOccurrence result = null;
        for (int i = 0; i < _ItemDelims.length; i++) {
            String delim = _ItemDelims[i];
            int index = _clause.toUpperCase().indexOf(delim, offset);
            if (index != -1) {
                if (result == null || index == Math.min(result.index, index)) {
                    result = new DelimOccurrence();
                    result.index = index;
                    result.delim = delim;
                }
            }
        }
        return result;
    }

    private void parseItem(String delim, String token) {
        if (token != null) {
            token = token.trim();
            if (!token.isEmpty()) {
                _processor.parse(delim, token);
            }
        }
    }
}
