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

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.FunctionType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

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
            itemToken = itemToken.substring(0, indexOfAlias);
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

        final FunctionType function;
        final int startParenthesis = expression.indexOf('(');
        if (startParenthesis > 0 && expression.endsWith(")")) {
            String functionName = expression.substring(0, startParenthesis);
            function = FunctionType.get(functionName);
            if (function != null) {
                expression = expression.substring(startParenthesis + 1, expression.length() - 1).trim();
                if (function == FunctionType.COUNT && "*".equals(expression)) {
                    return SelectItem.getCountAllItem();
                }
            }
        } else {
            function = null;
        }

        int lastIndexOfDot = expression.lastIndexOf(".");

        String columnName = null;
        FromItem fromItem = null;

        if (lastIndexOfDot != -1) {
            String prefix = expression.substring(0, lastIndexOfDot);
            columnName = expression.substring(lastIndexOfDot + 1);
            fromItem = _query.getFromClause().getItemByReference(prefix);
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
                if (column != null) {
                    SelectItem selectItem = new SelectItem(function, column, fromItem);
                    return selectItem;
                }
            } else if (fromItem.getSubQuery() != null) {
                final Query subQuery = fromItem.getSubQuery();
                final SelectItem subQuerySelectItem = new SelectItemParser(subQuery, _allowExpressionBasedSelectItems).findSelectItem(columnName);
                if (subQuerySelectItem == null) {
                    return null;
                }
                return new SelectItem(subQuerySelectItem, fromItem);
            }
        }

        if (_allowExpressionBasedSelectItems) {
            return new SelectItem(function, expression, null);
        }
        return null;
    }

}
