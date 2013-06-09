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
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.JoinType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class FromItemParser implements QueryPartProcessor {

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
            fromItem = parseJoinItem(itemToken);
        } else {
            fromItem = parseTableItem(itemToken);
        }

        _query.from(fromItem);
    }

    private FromItem parseTableItem(String itemToken) {
        final String[] tokens = itemToken.split(" ");
        final String alias;
        if (tokens.length == 2) {
            alias = tokens[1];
        } else if (tokens.length == 1) {
            alias = null;
        } else {
            throw new QueryParserException("Not capable of parsing FROM token: " + itemToken);
        }

        final Table table = _dataContext.getTableByQualifiedLabel(tokens[0]);
        if (table == null) {
            throw new QueryParserException("Not capable of parsing FROM token: " + itemToken);
        }

        final FromItem result = new FromItem(table);
        result.setAlias(alias);
        result.setQuery(_query);
        return result;
    }

    // this method will be documented based on this example itemToken: FOO f
    // INNER JOIN BAR b ON f.id = b.id
    private FromItem parseJoinItem(final String itemToken) {
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

            leftOn[i] = findSelectItem(leftPart, leftSide, rightSide);
            rightOn[i] = findSelectItem(rightPart, leftSide, rightSide);
        }

        final FromItem result = new FromItem(joinType, leftSide, rightSide, leftOn, rightOn);
        result.setQuery(_query);
        return result;
    }

    private SelectItem findSelectItem(String token, FromItem leftSide, FromItem rightSide) {
        // first look in the original query
        SelectItemParser selectItemParser = new SelectItemParser(_query, false);
        SelectItem result = selectItemParser.findSelectItem(token);

        if (result == null) {
            // fail over and try with the from items available in the join that
            // is being built.
            final Query temporaryQuery = new Query().from(leftSide, rightSide);
            selectItemParser = new SelectItemParser(temporaryQuery, false);
            result = selectItemParser.findSelectItem(token);

            if (result == null) {
                throw new QueryParserException("Not capable of parsing ON token: " + token);
            }

            // set the query on the involved query parts (since they have been
            // temporarily moved to the searched query).
            leftSide.setQuery(_query);
            rightSide.setQuery(_query);
            result.setQuery(_query);
        }
        return result;
    }

}
