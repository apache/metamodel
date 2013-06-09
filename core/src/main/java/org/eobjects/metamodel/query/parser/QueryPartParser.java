/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.query.parser;

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
            int index = _clause.indexOf(delim, offset);
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
