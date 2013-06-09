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

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.query.Query;

/**
 * A parser class of for full SQL-like queries.
 */
public class QueryParser {

    private final DataContext _dataContext;
    private final String _queryString;
    private final String _queryStringUpperCase;

    public QueryParser(DataContext dataContext, String queryString) {
        if (dataContext == null) {
            throw new IllegalArgumentException("DataContext cannot be null");
        }
        if (queryString == null) {
            throw new IllegalArgumentException("Query string cannot be null");
        }
        _dataContext = dataContext;
        _queryString = prepareQuery(queryString);
        _queryStringUpperCase = _queryString.toUpperCase();
    }

    /**
     * Performs any preparations (not changing any semantics) to the query
     * string
     * 
     * @param queryString
     * @return
     */
    private String prepareQuery(String queryString) {
        queryString = queryString.replaceAll("[\n\r\t]", " ");
        queryString = queryString.replaceAll("  ", " ");
        queryString = queryString.trim();
        return queryString;
    }

    public Query parse() throws QueryParserException {
        final Query query = new Query();

        // collect focal point query clauses
        int[] selectIndices = indexesOf("SELECT ", null);
        int[] fromIndices = indexesOf(" FROM ", selectIndices);
        int[] whereIndices = indexesOf(" WHERE ", fromIndices);
        int[] groupByIndices = indexesOf(" GROUP BY ", whereIndices);
        int[] havingIndices = indexesOf(" HAVING ", groupByIndices);
        int[] orderByIndices = indexesOf(" ORDER BY", havingIndices);
        int[] limitIndices = indexesOf(" LIMIT ", orderByIndices);
        int[] offsetIndices = indexesOf(" OFFSET ", limitIndices);

        // a few validations, minimum requirements
        if (selectIndices == null) {
            throw new QueryParserException("SELECT not found in query: " + _queryString);
        }
        if (fromIndices == null) {
            throw new QueryParserException("FROM not found in query: " + _queryString);
        }

        // parse FROM
        {
            final String fromClause = getSubstring(
                    getLastEndIndex(fromIndices),
                    getNextStartIndex(whereIndices, groupByIndices, havingIndices, orderByIndices, limitIndices,
                            offsetIndices));
            parseFromClause(query, fromClause);
        }

        {
            String selectClause = getSubstring(getLastEndIndex(selectIndices), fromIndices[0]);
            if (selectClause.startsWith("DISTINCT ")) {
                query.selectDistinct();
                selectClause = selectClause.substring("DISTINCT ".length());
            }
            parseSelectClause(query, selectClause);
        }

        if (whereIndices != null) {
            final String whereClause = getSubstring(getLastEndIndex(whereIndices),
                    getNextStartIndex(groupByIndices, havingIndices, orderByIndices, limitIndices, offsetIndices));
            if (whereClause != null) {
                parseWhereClause(query, whereClause);
            }
        }

        if (groupByIndices != null) {
            final String groupByClause = getSubstring(getLastEndIndex(groupByIndices, whereIndices),
                    getNextStartIndex(havingIndices, orderByIndices, limitIndices, offsetIndices));
            if (groupByClause != null) {
                parseGroupByClause(query, groupByClause);
            }
        }

        if (havingIndices != null) {
            final String havingClause = getSubstring(
                    getLastEndIndex(havingIndices, groupByIndices, whereIndices, fromIndices, selectIndices),
                    getNextStartIndex(orderByIndices, limitIndices, offsetIndices));
            if (havingClause != null) {
                parseHavingClause(query, havingClause);
            }
        }

        if (orderByIndices != null) {
            final String orderByClause = getSubstring(
                    getLastEndIndex(orderByIndices, havingIndices, groupByIndices, whereIndices, fromIndices,
                            selectIndices), getNextStartIndex(limitIndices, offsetIndices));
            if (orderByClause != null) {
                parseOrderByClause(query, orderByClause);
            }
        }

        if (limitIndices != null) {
            final String limitClause = getSubstring(
                    getLastEndIndex(limitIndices, orderByIndices, havingIndices, groupByIndices, whereIndices,
                            fromIndices, selectIndices), getNextStartIndex(offsetIndices));
            if (limitClause != null) {
                parseLimitClause(query, limitClause);
            }
        }

        if (offsetIndices != null) {
            final String offsetClause = getSubstring(
                    getLastEndIndex(offsetIndices, limitIndices, orderByIndices, havingIndices, groupByIndices,
                            whereIndices, fromIndices, selectIndices), getNextStartIndex());
            if (offsetClause != null) {
                parseOffsetClause(query, offsetClause);
            }
        }

        return query;
    }

    private void parseFromClause(Query query, String fromClause) {
        QueryPartParser clauseParser = new QueryPartParser(new FromItemParser(_dataContext, query), fromClause, ",");
        clauseParser.parse();
    }

    private void parseSelectClause(Query query, String selectClause) {
        QueryPartParser clauseParser = new QueryPartParser(new SelectItemParser(query, false), selectClause, ",");
        clauseParser.parse();
    }

    private void parseWhereClause(Query query, String whereClause) {
        // only parse "AND" delimitors, since "OR" will be taken care of as
        // compound filter items at 2nd level parsing
        QueryPartParser clauseParser = new QueryPartParser(new WhereItemParser(query), whereClause, " AND ");
        clauseParser.parse();
    }

    private void parseGroupByClause(Query query, String groupByClause) {
        QueryPartParser clauseParser = new QueryPartParser(new GroupByItemParser(query), groupByClause, ",");
        clauseParser.parse();
    }

    private void parseHavingClause(Query query, String havingClause) {
        // only parse "AND" delimitors, since "OR" will be taken care of as
        // compound filter items at 2nd level parsing
        QueryPartParser clauseParser = new QueryPartParser(new HavingItemParser(query), havingClause, " AND ");
        clauseParser.parse();
    }

    private void parseOrderByClause(Query query, String orderByClause) {
        QueryPartParser clauseParser = new QueryPartParser(new OrderByItemParser(query), orderByClause, ",");
        clauseParser.parse();
    }

    private void parseLimitClause(Query query, String limitClause) {
        limitClause = limitClause.trim();
        if (!limitClause.isEmpty()) {
            try {
                int limit = Integer.parseInt(limitClause);
                query.setMaxRows(limit);
            } catch (NumberFormatException e) {
                throw new QueryParserException("Could not parse LIMIT value: " + limitClause);
            }
        }
    }

    private void parseOffsetClause(Query query, String offsetClause) {
        offsetClause = offsetClause.trim();
        if (!offsetClause.isEmpty()) {
            try {
                final int offset = Integer.parseInt(offsetClause);
                // ofset is 0-based, but first-row is 1-based
                final int firstRow = offset + 1;
                query.setFirstRow(firstRow);
            } catch (NumberFormatException e) {
                throw new QueryParserException("Could not parse OFFSET value: " + offsetClause);
            }
        }
    }

    private String getSubstring(Integer from, int to) {
        if (from == null) {
            return null;
        }
        if (from.intValue() == to) {
            return null;
        }
        return _queryString.substring(from, to);
    }

    private int getNextStartIndex(int[]... indicesArray) {
        for (int[] indices : indicesArray) {
            if (indices != null) {
                return indices[0];
            }
        }
        return _queryString.length();
    }

    private Integer getLastEndIndex(int[]... indicesArray) {
        for (int[] indices : indicesArray) {
            if (indices != null) {
                return indices[1];
            }
        }
        return null;
    }

    /**
     * Finds the start and end indexes of a string in the query. The string
     * parameter of this method is expected to be in upper case, while the query
     * itself is tolerant of case differences.
     * 
     * @param string
     * @param previousIndices
     * @return
     */
    protected int[] indexesOf(String string, int[] previousIndices) {
        final int startIndex;
        if (previousIndices == null) {
            startIndex = _queryStringUpperCase.indexOf(string);
        } else {
            startIndex = _queryStringUpperCase.indexOf(string, previousIndices[1]);
        }
        if (startIndex == -1) {
            return null;
        }
        int endIndex = startIndex + string.length();
        return new int[] { startIndex, endIndex };
    }

}