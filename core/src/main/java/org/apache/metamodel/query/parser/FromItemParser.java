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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.JoinType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class FromItemParser implements QueryPartProcessor {
    
    /**
     * This field will hold start and end character for delimiter that can be
     * used
     */
    private static final Map<Character, Character> delimiterMap = new HashMap<Character, Character>();
    static {
        delimiterMap.put('\"', '\"');
        delimiterMap.put('[', ']');
    }
    
    private static final Logger logger = LoggerFactory.getLogger(FromItemParser.class);

    private final Query _query;
    private final DataContext _dataContext;

    public FromItemParser(DataContext dataContext, Query query) {
        _dataContext = dataContext;
        _query = query;
    }

    @Override
    public void parse(String delim, String itemToken) {
        final FromItem fromItem;

        final int parenthesisStart = itemToken.indexOf('(');
        if (parenthesisStart != -1) {
            if (parenthesisStart != 0) {
                throw new QueryParserException("Not capable of parsing FROM token: " + itemToken
                        + ". Expected parenthesis to start at first character.");
            }
            final int parenthesisEnd = itemToken.indexOf(')', parenthesisStart);
            if (parenthesisEnd == -1) {
                throw new QueryParserException("Not capable of parsing FROM token: " + itemToken
                        + ". Expected end parenthesis.");
            }

            final String subQueryString = itemToken.substring(parenthesisStart + 1, parenthesisEnd);
            logger.debug("Parsing sub-query: {}", subQueryString);

            final Query subQuery = new QueryParser(_dataContext, subQueryString).parse();
            fromItem = new FromItem(subQuery);

            final String alias = itemToken.substring(parenthesisEnd + 1).trim();
            if (!alias.isEmpty()) {
                fromItem.setAlias(alias);
            }
        } else if (itemToken.toUpperCase().indexOf(" JOIN ") != -1) {
            fromItem = parseAllJoinItems(itemToken);
        } else {
            fromItem = parseTableItem(itemToken);
        }

        _query.from(fromItem);
    }

    private FromItem parseTableItem(String itemToken) {
        // From token can be starting with [
        final String tableNameToken;
        final String aliasToken;

        char startDelimiter = itemToken.trim().charAt(0);
        if (delimiterMap.containsKey(startDelimiter)) {
            char endDelimiter = delimiterMap.get(startDelimiter);
            int endIndex = itemToken.trim().lastIndexOf(endDelimiter, itemToken.trim().length());
            if (endIndex <= 0) {
                throw new QueryParserException("Not capable of parsing FROM token: " + itemToken + ". Expected end "
                        + endDelimiter);
            }
            tableNameToken = itemToken.trim().substring(1, endIndex).trim();

            if (itemToken.trim().substring(1 + endIndex).trim().equalsIgnoreCase("")) {
                /*
                 * As per code in FromClause Method: getItemByReference(FromItem
                 * item, String reference) if (alias == null && table != null &&
                 * reference.equals(table.getName())) { Either we have to change
                 * the code to add alias.equals("") there or return null here.
                 */
                aliasToken = null;
            } else {
                aliasToken = itemToken.trim().substring(1 + endIndex).trim();
            }

        } else {
            // Default assumption is space being delimiter for tablename and
            // alias.. If otherwise use DoubleQuotes or [] around tableName
            final String[] tokens = itemToken.split(" ");
            tableNameToken = tokens[0];
            if (tokens.length == 2) {
                aliasToken = tokens[1];
            } else if (tokens.length == 1) {
                aliasToken = null;
            } else {
                throw new QueryParserException("Not capable of parsing FROM token: " + itemToken);
            }
        }

        final Table table = _dataContext.getTableByQualifiedLabel(tableNameToken);
        if (table == null) {
            throw new QueryParserException("Not capable of parsing FROM token: " + itemToken);
        }

        final FromItem result = new FromItem(table);
        result.setAlias(aliasToken);
        result.setQuery(_query);
        return result;
    }

    private FromItem parseAllJoinItems(final String itemToken) {
        String[] joinSplit = itemToken.split("(?i) JOIN ");
        List<String> joinsList = new ArrayList<String>();
        for (int i = 0; i < joinSplit.length - 1; i++) {
            joinSplit[i] = joinSplit[i].trim();
            joinSplit[i + 1] = joinSplit[i + 1].trim();
            String leftPart = joinSplit[i].substring(0, joinSplit[i].lastIndexOf(" "));
            String joinType = joinSplit[i].substring(joinSplit[i].lastIndexOf(" "));
            String rightPart = (i + 1 == joinSplit.length - 1) ? joinSplit[i + 1] : joinSplit[i + 1].substring(0,
                    joinSplit[i + 1].lastIndexOf(" "));
            joinsList.add((leftPart + " " + joinType + " JOIN " + rightPart).replaceAll(" +", " "));
            String rightTable = rightPart.substring(0, rightPart.toUpperCase().lastIndexOf(" ON "));
            String nextJoinType = joinSplit[i + 1].substring(joinSplit[i + 1].lastIndexOf(" "));
            joinSplit[i + 1] = rightTable + " " + nextJoinType;
        }
        Set<FromItem> fromItems = new HashSet<FromItem>();
        FromItem leftFromItem = null;
        for (String token : joinsList) {
            leftFromItem = parseJoinItem(leftFromItem, token, fromItems);
        }
        return leftFromItem;
    }

    // this method will be documented based on this example itemToken: FOO f
    // INNER JOIN BAR b ON f.id = b.id
    private FromItem parseJoinItem(final FromItem leftFromItem, final String itemToken, Set<FromItem> fromItems) {
        final int indexOfJoin = itemToken.toUpperCase().indexOf(" JOIN ");

        // firstPart = "FOO f INNER"
        final String firstPart = itemToken.substring(0, indexOfJoin).trim();

        // secondPart = "BAR b ON f.id = b.id"
        final String secondPart = itemToken.substring(indexOfJoin + " JOIN ".length()).trim();

        final int indexOfJoinType = firstPart.lastIndexOf(" ");

        // joinTypeString = "INNER"
        final String joinTypeString = firstPart.substring(indexOfJoinType).trim().toUpperCase();
        final JoinType joinType = JoinType.valueOf(joinTypeString);

        // firstTableToken = "FOO f"
        final String firstTableToken = firstPart.substring(0, indexOfJoinType).trim();

        final int indexOfOn = secondPart.toUpperCase().indexOf(" ON ");

        // secondTableToken = "BAR b"
        final String secondTableToken = secondPart.substring(0, indexOfOn).trim();

        final FromItem leftSide = parseTableItem(firstTableToken);
        final FromItem rightSide = parseTableItem(secondTableToken);

        fromItems.add(leftSide);
        fromItems.add(rightSide);

        // onClausess = ["f.id = b.id"]
        final String[] onClauses = secondPart.substring(indexOfOn + " ON ".length()).split(" AND ");
        final SelectItem[] leftOn = new SelectItem[onClauses.length];
        final SelectItem[] rightOn = new SelectItem[onClauses.length];
        for (int i = 0; i < onClauses.length; i++) {
            final String onClause = onClauses[i];
            final int indexOfEquals = onClause.indexOf("=");
            // leftPart = "f.id"
            final String leftPart = onClause.substring(0, indexOfEquals).trim();
            // rightPart = "b.id"
            final String rightPart = onClause.substring(indexOfEquals + 1).trim();

            leftOn[i] = findSelectItem(leftPart, fromItems.toArray(new FromItem[fromItems.size()]));
            rightOn[i] = findSelectItem(rightPart, fromItems.toArray(new FromItem[fromItems.size()]));
        }

        final FromItem leftItem = (leftFromItem != null) ? leftFromItem : leftSide;
        final FromItem result = new FromItem(joinType, leftItem, rightSide, leftOn, rightOn);
        result.setQuery(_query);
        return result;
    }

    private SelectItem findSelectItem(String token, FromItem[] joinTables) {
        // first look in the original query
        SelectItemParser selectItemParser = new SelectItemParser(_query, false);
        SelectItem result = selectItemParser.findSelectItem(token);

        if (result == null) {
            // fail over and try with the from items available in the join that
            // is being built.
            final Query temporaryQuery = new Query().from(joinTables);
            selectItemParser = new SelectItemParser(temporaryQuery, false);
            result = selectItemParser.findSelectItem(token);
            if (result == null) {
                throw new QueryParserException("Not capable of parsing ON token: " + token);
            }

            // set the query on the involved query parts (since they have been
            // temporarily moved to the searched query).
            result.setQuery(_query);
        }
        return result;
    }

}
