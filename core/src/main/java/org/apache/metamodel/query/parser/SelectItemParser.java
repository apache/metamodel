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

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.query.*;
import org.apache.metamodel.schema.Column;

public final class SelectItemParser implements QueryPartProcessor {

    public static class MultipleSelectItemsParsedException extends IllegalArgumentException {

        private static final long serialVersionUID = 1L;

        private final FromItem _fromItem;

        public MultipleSelectItemsParsedException(FromItem fromItem) {
            _fromItem = fromItem;
        }

        public FromItem getFromItem() {
            return _fromItem;
        }
    }

    private final Query _query;
    private final boolean _allowExpressionBasedSelectItems;

    public SelectItemParser(Query query, boolean allowExpressionBasedSelectItems) {
        _query = query;
        _allowExpressionBasedSelectItems = allowExpressionBasedSelectItems;
    }

    @Override
    public void parse(String delim, String itemToken) throws MetaModelException {
        if ("*".equals(itemToken)) {
            _query.selectAll();
            return;
        }

        String alias = null;
        final int indexOfAlias = itemToken.toUpperCase().lastIndexOf(" AS ");
        if (indexOfAlias != -1) {
            alias = itemToken.substring(indexOfAlias + " AS ".length());
            itemToken = itemToken.substring(0, indexOfAlias).trim();
        }

        try {
            final SelectItem selectItem = findSelectItem(itemToken);
            if (selectItem == null) {
                throw new QueryParserException("Not capable of parsing SELECT token: " + itemToken);
            }

            if (alias != null) {
                selectItem.setAlias(alias);
            }

            _query.select(selectItem);
        } catch (MultipleSelectItemsParsedException e) {
            FromItem fromItem = e.getFromItem();
            if (fromItem != null) {
                _query.selectAll(fromItem);
            } else {
                throw e;
            }
        }
    }

    /**
     * Finds/creates a SelectItem based on the given expression. Unlike the
     * {@link #parse(String, String)} method, this method will not actually add
     * the selectitem to the query.
     * 
     * @param expression
     * @return
     * 
     * @throws MultipleSelectItemsParsedException
     *             if an expression yielding multiple select-items (such as "*")
     *             was passed in the expression
     */
    public SelectItem findSelectItem(String expression) throws MultipleSelectItemsParsedException {
        if ("*".equals(expression)) {
            throw new MultipleSelectItemsParsedException(null);
        }

        if ("COUNT(*)".equalsIgnoreCase(expression)) {
            return SelectItem.getCountAllItem();
        }

        final String unmodifiedExpression = expression;

        final boolean functionApproximation;
        final FunctionType function;
        final int startParenthesis = expression.indexOf('(');
        if (startParenthesis > 0 && expression.endsWith(")")) {
            functionApproximation = (expression.startsWith(SelectItem.FUNCTION_APPROXIMATION_PREFIX));
            final String functionName = expression.substring(
                    (functionApproximation ? SelectItem.FUNCTION_APPROXIMATION_PREFIX.length() : 0), startParenthesis);
            function = FunctionTypeFactory.get(functionName.toUpperCase());
            if (function != null) {
                expression = expression.substring(startParenthesis + 1, expression.length() - 1).trim();
                if (function instanceof CountAggregateFunction && "*".equals(expression)) {
                    final SelectItem selectItem = SelectItem.getCountAllItem();
                    selectItem.setFunctionApproximationAllowed(functionApproximation);
                    return selectItem;
                }
            }
        } else {
            function = null;
            functionApproximation = false;
        }

        String columnName = null;
        FromItem fromItem = null;

        // attempt to find from item by cutting up the string in prefix and
        // suffix around dot.
        {
            int splitIndex = expression.lastIndexOf('.');
            while (fromItem == null && splitIndex != -1) {
                final String prefix = expression.substring(0, splitIndex);
                columnName = expression.substring(splitIndex + 1);
                fromItem = _query.getFromClause().getItemByReference(prefix);

                splitIndex = expression.lastIndexOf('.', splitIndex - 1);
            }
        }

        if (fromItem == null) {
            if (_query.getFromClause().getItemCount() == 1) {
                fromItem = _query.getFromClause().getItem(0);
                columnName = expression;
            } else {
                fromItem = null;
                columnName = null;
            }
        }

        if (fromItem != null) {
            if ("*".equals(columnName)) {
                throw new MultipleSelectItemsParsedException(fromItem);
            } else if (fromItem.getTable() != null) {
                Column column = fromItem.getTable().getColumnByName(columnName);
                int offset = -1;
                while (function == null && column == null) {
                    // check for MAP_VALUE shortcut syntax
                    offset = columnName.indexOf('.', offset + 1);
                    if (offset == -1) {
                        break;
                    }

                    final String part1 = columnName.substring(0, offset);
                    column = fromItem.getTable().getColumnByName(part1);
                    if (column != null) {
                        final String part2 = columnName.substring(offset + 1);
                        return new SelectItem(new MapValueFunction(), new Object[] { part2 }, column, fromItem);
                    }
                }

                if (column != null) {
                    final SelectItem selectItem = new SelectItem(function, column, fromItem);
                    selectItem.setFunctionApproximationAllowed(functionApproximation);
                    return selectItem;
                }
            } else if (fromItem.getSubQuery() != null) {
                final Query subQuery = fromItem.getSubQuery();
                final SelectItem subQuerySelectItem = new SelectItemParser(subQuery, _allowExpressionBasedSelectItems)
                        .findSelectItem(columnName);
                if (subQuerySelectItem == null) {
                    return null;
                }
                return new SelectItem(subQuerySelectItem, fromItem);
            }
        }

        // if the expression is alias of some select item defined return that
        // select item
        final SelectItem aliasSelectItem = MetaModelHelper.getSelectItemByAlias(_query, unmodifiedExpression);
        if (aliasSelectItem != null) {
            return aliasSelectItem;
        }

        if (_allowExpressionBasedSelectItems) {
            final SelectItem selectItem = new SelectItem(function, expression, null);
            selectItem.setFunctionApproximationAllowed(functionApproximation);
            return selectItem;
        }
        return null;
    }
}
