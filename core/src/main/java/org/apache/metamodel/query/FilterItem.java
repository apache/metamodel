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
package org.apache.metamodel.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.metamodel.data.IRowFilter;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.BaseObject;
import org.apache.metamodel.util.CollectionUtils;
import org.apache.metamodel.util.FormatHelper;
import org.apache.metamodel.util.ObjectComparator;
import org.apache.metamodel.util.WildcardPattern;

/**
 * Represents a filter in a query that resides either within a WHERE clause or a
 * HAVING clause
 *
 * @see FilterClause
 * @see OperatorType
 * @see LogicalOperator
 */
public class FilterItem extends BaseObject implements QueryItem, Cloneable, IRowFilter {

    private static final long serialVersionUID = 2435322742894653227L;

    private Query _query;
    private final SelectItem _selectItem;
    private final OperatorType _operator;
    private final Object _operand;
    private final List<FilterItem> _childItems;
    private final LogicalOperator _logicalOperator;
    private final String _expression;
    private transient Set<?> _inValues;

    /**
     * Private constructor, used for cloning
     */
    private FilterItem(SelectItem selectItem, OperatorType operator, Object operand, List<FilterItem> orItems,
                       String expression, LogicalOperator logicalOperator) {
        _selectItem = selectItem;
        _operator = operator;
        _operand = validateOperand(operand);
        _childItems = orItems;
        _expression = expression;
        _logicalOperator = logicalOperator;
    }

    private Object validateOperand(Object operand) {
        if (operand instanceof Column) {
            // gracefully convert to a select item.
            operand = new SelectItem((Column) operand);
        }
        return operand;
    }

    /**
     * Creates a single filter item based on a SelectItem, an operator and an
     * operand.
     *
     * @param selectItem
     *            the selectItem to put constraints on, cannot be null
     * @param operator
     *            The operator to use. Can be OperatorType.EQUALS_TO,
     *            OperatorType.DIFFERENT_FROM,
     *            OperatorType.GREATER_THAN,OperatorType.LESS_THAN
     *            OperatorType.GREATER_THAN_OR_EQUAL,
     *            OperatorType.LESS_THAN_OR_EQUAL
     * @param operand
     *            The operand. Can be a constant like null or a String, a
     *            Number, a Boolean, a Date, a Time, a DateTime. Or another
     *            SelectItem
     * @throws IllegalArgumentException
     *             if the SelectItem is null or if the combination of operator
     *             and operand does not make sense.
     */
    public FilterItem(SelectItem selectItem, OperatorType operator, Object operand) throws IllegalArgumentException {
        this(selectItem, operator, operand, null, null, null);
        if (_operand == null) {
            require("Can only use EQUALS or DIFFERENT_FROM operator with null-operand",
                    _operator == OperatorType.DIFFERENT_FROM || _operator == OperatorType.EQUALS_TO);
        }
        if (_operator == OperatorType.LIKE || _operator == OperatorType.NOT_LIKE) {
            ColumnType type = _selectItem.getColumn().getType();
            if (type != null) {
                require("Can only use LIKE operator with strings", type.isLiteral()
                        && (_operand instanceof String || _operand instanceof SelectItem));
            }
        }
        require("SelectItem cannot be null", _selectItem != null);
    }

    /**
     * Creates a single unvalidated filter item based on a expression.
     * Expression based filters are typically NOT datastore-neutral but are
     * available for special "hacking" needs.
     *
     * Expression based filters can only be used for JDBC based datastores since
     * they are translated directly into SQL.
     *
     * @param expression
     *            An expression to use for the filter, for example
     *            "YEAR(my_date) = 2008".
     */
    public FilterItem(String expression) {
        this(null, null, null, null, expression, null);

        require("Expression cannot be null", _expression != null);
    }

    /**
     * Creates a composite filter item based on other filter items. Each
     * provided filter items will be OR'ed meaning that if one of the evaluates
     * as true, then the composite filter will be evaluated as true
     *
     * @param items
     *            a list of items to include in the composite
     */
    public FilterItem(List<FilterItem> items) {
        this(LogicalOperator.OR, items);
    }

    /**
     * Creates a compound filter item based on other filter items. Each provided
     * filter item will be combined according to the {@link LogicalOperator}.
     *
     * @param logicalOperator
     *            the logical operator to apply
     * @param items
     *            a list of items to include in the composite
     */
    public FilterItem(LogicalOperator logicalOperator, List<FilterItem> items) {
        this(null, null, null, items, null, logicalOperator);

        require("Child items cannot be null", _childItems != null);
        require("Child items cannot be empty", !_childItems.isEmpty());
    }

    /**
     * Creates a compound filter item based on other filter items. Each provided
     * filter item will be combined according to the {@link LogicalOperator}.
     *
     * @param logicalOperator
     *            the logical operator to apply
     * @param items
     *            an array of items to include in the composite
     */
    public FilterItem(LogicalOperator logicalOperator, FilterItem... items) {
        this(logicalOperator, Arrays.asList(items));
    }

    /**
     * Creates a compound filter item based on other filter items. Each provided
     * filter items will be OR'ed meaning that if one of the evaluates as true,
     * then the compound filter will be evaluated as true
     *
     * @param items
     *            an array of items to include in the composite
     */
    public FilterItem(FilterItem... items) {
        this(Arrays.asList(items));
    }

    private void require(String errorMessage, boolean b) {
        if (!b) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    public SelectItem getSelectItem() {
        return _selectItem;
    }

    public OperatorType getOperator() {
        return _operator;
    }

    public Object getOperand() {
        return _operand;
    }

    public String getExpression() {
        return _expression;
    }

    public Query getQuery() {
        return _query;
    }

    public LogicalOperator getLogicalOperator() {
        return _logicalOperator;
    }

    public FilterItem setQuery(Query query) {
        _query = query;
        if (_childItems == null) {
            if (_expression == null) {
                if (_selectItem.getQuery() == null) {
                    _selectItem.setQuery(_query);
                }
                if (_operand instanceof SelectItem) {
                    SelectItem operand = (SelectItem) _operand;
                    if (operand.getQuery() == null) {
                        operand.setQuery(_query);
                    }
                }
            }
        } else {
            for (FilterItem item : _childItems) {
                if (item.getQuery() == null) {
                    item.setQuery(_query);
                }
            }
        }
        return this;
    }

    @Override
    public String toSql() {
        return toSql(false);
    }

    /**
     * Parses the constraint as a SQL Where-clause item
     */
    @Override
    public String toSql(boolean includeSchemaInColumnPaths) {
        if (_expression != null) {
            return _expression;
        }

        StringBuilder sb = new StringBuilder();
        if (_childItems == null) {
            sb.append(_selectItem.getSameQueryAlias(includeSchemaInColumnPaths));

            if (_operand == null && _operator == OperatorType.EQUALS_TO) {
                sb.append(" IS NULL");
            } else if (_operand == null && _operator == OperatorType.DIFFERENT_FROM) {
                sb.append(" IS NOT NULL");
            } else {
                final Object operand = appendOperator(sb, _operand, _operator);

                if (operand instanceof SelectItem) {
                    final String selectItemString = ((SelectItem) operand)
                            .getSameQueryAlias(includeSchemaInColumnPaths);
                    sb.append(selectItemString);
                } else {
                    ColumnType columnType = _selectItem.getExpectedColumnType();
                    final String sqlValue = FormatHelper.formatSqlValue(columnType, operand);
                    sb.append(sqlValue);
                }
            }
        } else {
            sb.append('(');
            for (int i = 0; i < _childItems.size(); i++) {
                FilterItem item = _childItems.get(i);
                if (i != 0) {
                    sb.append(' ');
                    sb.append(_logicalOperator.toString());
                    sb.append(' ');
                }
                sb.append(item.toSql());
            }
            sb.append(')');
        }

        return sb.toString();
    }

    public static Object appendOperator(StringBuilder sb, Object operand, OperatorType operator) {
        sb.append(' ');
        sb.append(operator.toSql());
        sb.append(' ');

        if (operator == OperatorType.IN || operator == OperatorType.NOT_IN) {
            operand = CollectionUtils.toList(operand);
        }
        return operand;
    }

    /**
     * Does a "manual" evaluation, useful for CSV data and alike, where queries
     * cannot be created.
     */
    public boolean evaluate(Row row) {
        require("Expression-based filters cannot be manually evaluated", _expression == null);

        if (_childItems == null) {
            // Evaluate a single constraint
            Object selectItemValue = row.getValue(_selectItem);
            Object operandValue = _operand;
            if (_operand instanceof SelectItem) {
                SelectItem selectItem = (SelectItem) _operand;
                operandValue = row.getValue(selectItem);
            }
            if (operandValue == null) {
                if (_operator == OperatorType.DIFFERENT_FROM) {
                    return (selectItemValue != null);
                } else if (_operator == OperatorType.EQUALS_TO) {
                    return (selectItemValue == null);
                } else {
                    return false;
                }
            } else if (selectItemValue == null) {
                if (_operator == OperatorType.DIFFERENT_FROM) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return compare(selectItemValue, operandValue);
            }
        } else {

            // Evaluate several constraints
            if (_logicalOperator == LogicalOperator.AND) {
                // require all results to be true
                for (FilterItem item : _childItems) {
                    boolean result = item.evaluate(row);
                    if (!result) {
                        return false;
                    }
                }
                return true;
            } else {
                // require at least one result to be true
                for (FilterItem item : _childItems) {
                    boolean result = item.evaluate(row);
                    if (result) {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    private boolean compare(Object selectItemValue, Object operandValue) {
        Comparator<Object> comparator = ObjectComparator.getComparator();
        if (_operator == OperatorType.DIFFERENT_FROM) {
            return comparator.compare(selectItemValue, operandValue) != 0;
        } else if (_operator == OperatorType.EQUALS_TO) {
            return comparator.compare(selectItemValue, operandValue) == 0;
        } else if (_operator == OperatorType.GREATER_THAN) {
            return comparator.compare(selectItemValue, operandValue) > 0;
        } else if (_operator == OperatorType.GREATER_THAN_OR_EQUAL) {
            return comparator.compare(selectItemValue, operandValue) >= 0;
        } else if (_operator == OperatorType.LESS_THAN) {
            return comparator.compare(selectItemValue, operandValue) < 0;
        } else if (_operator == OperatorType.LESS_THAN_OR_EQUAL) {
            return comparator.compare(selectItemValue, operandValue) <= 0;
        } else if (_operator == OperatorType.LIKE) {
            WildcardPattern matcher = new WildcardPattern((String) operandValue, '%');
            return matcher.matches((String) selectItemValue);
        } else if (_operator == OperatorType.NOT_LIKE) {
            WildcardPattern matcher = new WildcardPattern((String) operandValue, '%');
            return !matcher.matches((String) selectItemValue);
        } else if (_operator == OperatorType.IN) {
            Set<?> inValues = getInValues();
            return inValues.contains(selectItemValue);
        } else if (_operator == OperatorType.NOT_IN) {
            Set<?> inValues = getInValues();
            return !inValues.contains(selectItemValue);
        } else {
            throw new IllegalStateException("Operator could not be determined");
        }
    }

    /**
     * Lazy initializes a set (for fast searching) of IN values.
     *
     * @return a hash set appropriate for IN clause evaluation
     */
    private Set<?> getInValues() {
        if (_inValues == null) {
            if (_operand instanceof Set) {
                _inValues = (Set<?>) _operand;
            } else {
                List<?> list = CollectionUtils.toList(_operand);
                _inValues = new HashSet<Object>(list);
            }
        }
        return _inValues;
    }

    @Override
    protected FilterItem clone() {
        final List<FilterItem> orItems;
        if (_childItems == null) {
            orItems = null;
        } else {
            orItems = new ArrayList<FilterItem>(_childItems);
        }

        final Object operand;
        if (_operand instanceof SelectItem) {
            operand = ((SelectItem) _operand).clone();
        } else {
            operand = _operand;
        }

        final SelectItem selectItem;
        if (_selectItem == null) {
            selectItem = null;
        } else {
            selectItem = _selectItem.clone();
        }

        return new FilterItem(selectItem, _operator, operand, orItems, _expression, _logicalOperator);
    }

    public boolean isReferenced(Column column) {
        if (column != null) {
            if (_selectItem != null) {
                if (_selectItem.isReferenced(column)) {
                    return true;
                }
            }
            if (_operand != null && _operand instanceof SelectItem) {
                if (((SelectItem) _operand).isReferenced(column)) {
                    return true;
                }
            }
            if (_childItems != null) {
                for (FilterItem item : _childItems) {
                    if (item.isReferenced(column)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(_expression);
        identifiers.add(_operand);
        identifiers.add(_childItems);
        identifiers.add(_operator);
        identifiers.add(_selectItem);
        identifiers.add(_logicalOperator);
    }

    /**
     * Gets the {@link FilterItem}s that this filter item consists of, if it is
     * a compound filter item.
     *
     * @deprecated use {@link #getChildItems()} instead
     */
    @Deprecated
    public FilterItem[] getOrItems() {
        return getChildItems();
    }

    /**
     * Gets the number of child items, if this is a compound filter item.
     *
     * @deprecated use {@link #getChildItemCount()} instead.
     */
    @Deprecated
    public int getOrItemCount() {
        return getChildItemCount();
    }

    /**
     * Get the number of child items, if this is a compound filter item.
     */
    public int getChildItemCount() {
        if (_childItems == null) {
            return 0;
        }
        return _childItems.size();
    }

    /**
     * Gets the {@link FilterItem}s that this filter item consists of, if it is
     * a compound filter item.
     */
    public FilterItem[] getChildItems() {
        if (_childItems == null) {
            return null;
        }
        return _childItems.toArray(new FilterItem[_childItems.size()]);
    }

    /**
     * Determines whether this {@link FilterItem} is a compound filter or not
     * (ie. if it has child items or not)
     */
    public boolean isCompoundFilter() {
        return _childItems != null;
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean accept(Row row) {
        return evaluate(row);
    }
}
