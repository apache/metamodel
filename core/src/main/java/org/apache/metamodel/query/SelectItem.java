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

import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.BaseObject;
import org.apache.metamodel.util.EqualsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a SELECT item. SelectItems can take different forms:
 * <ul>
 * <li>column SELECTs (selects a column from a table)</li>
 * <li>column function SELECTs (aggregates the values of a column)</li>
 * <li>expression SELECTs (retrieves data based on an expression (only supported
 * for JDBC datastores)</li>
 * <li>expression function SELECTs (retrieves databased on a function and an
 * expression, only COUNT(*) is supported for non-JDBC datastores))</li>
 * <li>SELECTs from subqueries (works just like column selects, but in stead of
 * pointing to a column, it retrieves data from the select item of a subquery)</li>
 * </ul>
 * 
 * @see SelectClause
 */
public class SelectItem extends BaseObject implements QueryItem, Cloneable {

    public static final String FUNCTION_APPROXIMATION_PREFIX = "APPROXIMATE ";

    private static final long serialVersionUID = 317475105509663973L;
    private static final Logger logger = LoggerFactory.getLogger(SelectItem.class);

    // immutable fields (essense)
    private final Column _column;
    private final FunctionType _function;
    private final Object[] _functionParameters;
    private final String _expression;
    private final SelectItem _subQuerySelectItem;
    private final FromItem _fromItem;

    // mutable fields (tweaking)
    private boolean _functionApproximationAllowed;
    private Query _query;
    private String _alias;

    /**
     * All-arguments constructor
     * 
     * @param column
     * @param fromItem
     * @param function
     * @param functionParameters
     * @param expression
     * @param subQuerySelectItem
     * @param alias
     * @param functionApproximationAllowed
     */
    private SelectItem(Column column, FromItem fromItem, FunctionType function, Object[] functionParameters,
            String expression, SelectItem subQuerySelectItem, String alias, boolean functionApproximationAllowed) {
        super();
        _column = column;
        _fromItem = fromItem;
        _function = function;
        _functionParameters = functionParameters;
        _expression = expression;
        _subQuerySelectItem = subQuerySelectItem;
        _alias = alias;
        _functionApproximationAllowed = functionApproximationAllowed;
    }

    /**
     * Generates a COUNT(*) select item
     */
    public static SelectItem getCountAllItem() {
        return new SelectItem(FunctionType.COUNT, "*", null);
    }

    public static boolean isCountAllItem(SelectItem item) {
        if (item != null && item.getAggregateFunction() != null && item.getAggregateFunction().toString().equals("COUNT")
                && item.getExpression() == "*") {
            return true;
        }
        return false;
    }

    /**
     * Creates a simple SelectItem that selects from a column
     * 
     * @param column
     */
    public SelectItem(Column column) {
        this(null, column);
    }

    /**
     * Creates a SelectItem that uses a function on a column, for example
     * SUM(price) or MAX(age)
     * 
     * @param function
     * @param column
     */
    public SelectItem(FunctionType function, Column column) {
        this(function, column, null);
    }

    /**
     * Create a SelectItem that uses a function with parameters on a column.
     * 
     * @param function
     * @param functionParameters
     * @param column
     */
    public SelectItem(FunctionType function, Object[] functionParameters, Column column) {
        this(function, functionParameters, column, null);
    }

    /**
     * Creates a SelectItem that references a column from a particular
     * {@link FromItem}, for example a.price or p.age
     * 
     * @param column
     * @param fromItem
     */
    public SelectItem(Column column, FromItem fromItem) {
        this(null, column, fromItem);
        if (fromItem != null) {
            Table fromItemTable = fromItem.getTable();
            if (fromItemTable != null) {
                Table columnTable = column.getTable();
                if (columnTable != null && !columnTable.equals(fromItemTable)) {
                    throw new IllegalArgumentException("Column's table '" + columnTable.getName()
                            + "' is not equal to referenced table: " + fromItemTable);
                }
            }
        }
    }

    /**
     * Creates a SelectItem that uses a function on a column from a particular
     * {@link FromItem}, for example SUM(a.price) or MAX(p.age)
     * 
     * @param function
     * @param column
     * @param fromItem
     */
    public SelectItem(FunctionType function, Column column, FromItem fromItem) {
        this(column, fromItem, function, null, null, null, null, false);
        if (column == null) {
            throw new IllegalArgumentException("column=null");
        }
    }

    /**
     * Creates a SelectItem that uses a function with parameters on a column
     * from a particular {@link FromItem}, for example
     * MAP_VALUE('path.to.value', doc)
     * 
     * @param function
     * @param functionParameters
     * @param column
     * @param fromItem
     */
    public SelectItem(FunctionType function, Object[] functionParameters, Column column, FromItem fromItem) {
        this(column, fromItem, function, functionParameters, null, null, null, false);
        if (column == null) {
            throw new IllegalArgumentException("column=null");
        }
    }

    /**
     * Creates a SelectItem based on an expression. All expression-based
     * SelectItems must have aliases.
     * 
     * @param expression
     * @param alias
     */
    public SelectItem(String expression, String alias) {
        this(null, expression, alias);
    }

    /**
     * Creates a SelectItem based on a function and an expression. All
     * expression-based SelectItems must have aliases.
     * 
     * @param function
     * @param expression
     * @param alias
     */
    public SelectItem(FunctionType function, String expression, String alias) {
        this(null, null, function, null, expression, null, alias, false);
        if (expression == null) {
            throw new IllegalArgumentException("expression=null");
        }
    }

    /**
     * Creates a SelectItem that references another select item in a subquery
     * 
     * @param subQuerySelectItem
     * @param subQueryFromItem
     *            the FromItem that holds the sub-query
     */
    public SelectItem(SelectItem subQuerySelectItem, FromItem subQueryFromItem) {
        this(null, subQueryFromItem, null, null, null, subQuerySelectItem, null, false);
        if (subQueryFromItem.getSubQuery() == null) {
            throw new IllegalArgumentException("Only sub-query based FromItems allowed.");
        }
        if (subQuerySelectItem.getQuery() != null
                && !subQuerySelectItem.getQuery().equals(subQueryFromItem.getSubQuery())) {
            throw new IllegalArgumentException("The SelectItem must exist in the sub-query");
        }
    }

    public String getAlias() {
        return _alias;
    }

    public SelectItem setAlias(String alias) {
        _alias = alias;
        return this;
    }

    public boolean hasFunction(){
        return _function != null;
    }

    public AggregateFunction getAggregateFunction() {
        if (_function instanceof AggregateFunction) {
            return (AggregateFunction) _function;
        }
        return null;
    }

    public ScalarFunction getScalarFunction() {
        if (_function instanceof ScalarFunction) {
            return (ScalarFunction) _function;
        }
        return null;
    }

    /**
     * Gets any parameters to the {@link #getFunction()} used.
     * 
     * @return
     */
    public Object[] getFunctionParameters() {
        return _functionParameters;
    }

    /**
     * @return if this is a function based SelectItem where function calculation
     *         is allowed to be approximated (if the datastore type has an
     *         approximate calculation method). Approximated function results
     *         are as the name implies not exact, but might be valuable as an
     *         optimization in some cases.
     */
    public boolean isFunctionApproximationAllowed() {
        return _functionApproximationAllowed;
    }

    public void setFunctionApproximationAllowed(boolean functionApproximationAllowed) {
        _functionApproximationAllowed = functionApproximationAllowed;
    }

    public Column getColumn() {
        return _column;
    }

    /**
     * Tries to infer the {@link ColumnType} of this {@link SelectItem}. For
     * expression based select items, this is not possible, and the method will
     * return null.
     * 
     * @return
     */
    public ColumnType getExpectedColumnType() {
        if (_subQuerySelectItem != null) {
            return _subQuerySelectItem.getExpectedColumnType();
        }
        if (_function != null) {
            if (_column != null) {
                return _function.getExpectedColumnType(_column.getType());
            } else {
                return _function.getExpectedColumnType(null);
            }
        }
        if (_column != null) {
            return _column.getType();
        }
        return null;
    }

    /**
     * Returns an "expression" that this select item represents. Expressions are
     * not necesarily portable across {@link DataContext} implementations, but
     * may be useful for utilizing database-specific behaviour in certain cases.
     * 
     * @return
     */
    public String getExpression() {
        return _expression;
    }

    public SelectItem setQuery(Query query) {
        _query = query;
        return this;
    }

    public Query getQuery() {
        return _query;
    }

    public SelectItem getSubQuerySelectItem() {
        return _subQuerySelectItem;
    }

    public FromItem getFromItem() {
        return _fromItem;
    }

    /**
     * @return the name that this SelectItem can be referenced with, if
     *         referenced from a super-query. This will usually be the alias,
     *         but if there is no alias, then the column name will be used.
     */
    public String getSuperQueryAlias() {
        return getSuperQueryAlias(true);
    }

    /**
     * @return the name that this SelectItem can be referenced with, if
     *         referenced from a super-query. This will usually be the alias,
     *         but if there is no alias, then the column name will be used.
     * 
     * @param includeQuotes
     *            indicates whether or not the output should include quotes, if
     *            the select item's column has quotes associated (typically
     *            true, but false if used for presentation)
     */
    public String getSuperQueryAlias(boolean includeQuotes) {
        if (_alias != null) {
            return _alias;
        } else if (_column != null) {
            final StringBuilder sb = new StringBuilder();
            if (_function != null) {
                if (_functionApproximationAllowed) {
                    sb.append(FUNCTION_APPROXIMATION_PREFIX);
                }
                sb.append(_function.getFunctionName());
                sb.append('(');
            }
            if (includeQuotes) {
                sb.append(_column.getQuotedName());
            } else {
                sb.append(_column.getName());
            }
            if (_function != null) {
                sb.append(')');
            }
            return sb.toString();
        } else {
            logger.debug("Could not resolve a reasonable super-query alias for SelectItem: {}", toSql());
            return toStringNoAlias().toString();
        }
    }

    public String getSameQueryAlias() {
        return getSameQueryAlias(false);
    }

    /**
     * @return an alias that can be used in WHERE, GROUP BY and ORDER BY clauses
     *         in the same query
     */
    public String getSameQueryAlias(boolean includeSchemaInColumnPath) {
        if (_column != null) {
            StringBuilder sb = new StringBuilder();
            String columnPrefix = getToStringColumnPrefix(includeSchemaInColumnPath);
            sb.append(columnPrefix);
            sb.append(_column.getQuotedName());
            if (_function != null) {
                if (_functionApproximationAllowed) {
                    sb.insert(0, FUNCTION_APPROXIMATION_PREFIX + _function.getFunctionName() + "(");
                } else {
                    sb.insert(0, _function.getFunctionName() + "(");
                }
                sb.append(")");
            }
            return sb.toString();
        }
        String alias = getAlias();
        if (alias == null) {
            alias = toStringNoAlias(includeSchemaInColumnPath).toString();
            logger.debug("Could not resolve a reasonable same-query alias for SelectItem: {}", toSql());
        }
        return alias;
    }

    @Override
    public String toSql() {
        return toSql(false);
    }

    @Override
    public String toSql(boolean includeSchemaInColumnPath) {
        final StringBuilder sb = toStringNoAlias(includeSchemaInColumnPath);
        if (_alias != null) {
            sb.append(" AS ");
            sb.append(_alias);
        }
        return sb.toString();
    }

    public StringBuilder toStringNoAlias() {
        return toStringNoAlias(false);
    }

    public StringBuilder toStringNoAlias(boolean includeSchemaInColumnPath) {
        final StringBuilder sb = new StringBuilder();
        if (_column != null) {
            sb.append(getToStringColumnPrefix(includeSchemaInColumnPath));
            sb.append(_column.getQuotedName());
        }
        if (_expression != null) {
            sb.append(_expression);
        }
        if (_fromItem != null && _subQuerySelectItem != null) {
            if (_fromItem.getAlias() != null) {
                sb.append(_fromItem.getAlias() + '.');
            }
            sb.append(_subQuerySelectItem.getSuperQueryAlias());
        }
        if (_function != null) {
            final StringBuilder functionBeginning = new StringBuilder();
            if (_functionApproximationAllowed) {
                functionBeginning.append(FUNCTION_APPROXIMATION_PREFIX);
            }

            functionBeginning.append(_function.getFunctionName());
            functionBeginning.append('(');
            final Object[] functionParameters = getFunctionParameters();
            if (functionParameters != null && functionParameters.length != 0) {
                for (int i = 0; i < functionParameters.length; i++) {
                    functionBeginning.append('\'');
                    functionBeginning.append(functionParameters[i]);
                    functionBeginning.append('\'');
                    functionBeginning.append(',');
                }
            }
            sb.insert(0, functionBeginning.toString());
            sb.append(")");
        }
        return sb;
    }

    private String getToStringColumnPrefix(boolean includeSchemaInColumnPath) {
        final StringBuilder sb = new StringBuilder();
        if (_fromItem != null && _fromItem.getAlias() != null) {
            sb.append(_fromItem.getAlias());
            sb.append('.');
        } else {
            final Table table = _column.getTable();
            String tableLabel;
            if (_query == null) {
                tableLabel = null;
            } else {
                tableLabel = _query.getFromClause().getAlias(table);
            }
            if (table != null) {
                if (tableLabel == null) {
                    tableLabel = table.getQuotedName();
                    if (includeSchemaInColumnPath) {
                        Schema schema = table.getSchema();
                        if (schema != null) {
                            tableLabel = schema.getQuotedName() + "." + tableLabel;
                        }
                    }
                }
                sb.append(tableLabel);
                sb.append('.');
            }
        }
        return sb.toString();
    }

    public boolean equalsIgnoreAlias(SelectItem that) {
        return equalsIgnoreAlias(that, false);
    }

    public boolean equalsIgnoreAlias(SelectItem that, boolean exactColumnCompare) {
        if (that == null) {
            return false;
        }
        if (that == this) {
            return true;
        }

        EqualsBuilder eb = new EqualsBuilder();
        if (exactColumnCompare) {
            eb.append(this._column == that._column);
            eb.append(this._fromItem, that._fromItem);
        } else {
            eb.append(this._column, that._column);
        }
        eb.append(this._function, that._function);
        eb.append(this._functionApproximationAllowed, that._functionApproximationAllowed);
        eb.append(this._expression, that._expression);
        if (_subQuerySelectItem != null) {
            eb.append(_subQuerySelectItem.equalsIgnoreAlias(that._subQuerySelectItem));
        } else {
            if (that._subQuerySelectItem != null) {
                eb.append(false);
            }
        }
        return eb.isEquals();
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(_expression);
        identifiers.add(_alias);
        identifiers.add(_column);
        identifiers.add(_function);
        identifiers.add(_functionApproximationAllowed);
        if (_fromItem == null && _column != null && _column.getTable() != null) {
            // add a FromItem representing the column's table - this makes equal
            // comparison work when the only difference is whether or not
            // FromItem is specified
            identifiers.add(new FromItem(_column.getTable()));
        } else {
            identifiers.add(_fromItem);
        }
        identifiers.add(_subQuerySelectItem);
    }

    @Override
    protected SelectItem clone() {
        return clone(null);
    }

    /**
     * Creates a clone of the {@link SelectItem} for use within a cloned
     * {@link Query}.
     * 
     * @param clonedQuery
     *            a new {@link Query} object that represents the clone-to-be of
     *            a query. It is expected that {@link FromItem}s have already
     *            been cloned in this {@link Query}.
     * @return
     */
    protected SelectItem clone(Query clonedQuery) {
        final SelectItem subQuerySelectItem = (_subQuerySelectItem == null ? null : _subQuerySelectItem.clone());
        final FromItem fromItem;
        if (_fromItem == null) {
            fromItem = null;
        } else if (clonedQuery != null && _query != null) {
            final int indexOfFromItem = _query.getFromClause().indexOf(_fromItem);
            if (indexOfFromItem != -1) {
                fromItem = clonedQuery.getFromClause().getItem(indexOfFromItem);
            } else {
                fromItem = _fromItem.clone();
            }
        } else {
            fromItem = _fromItem.clone();
        }

        final SelectItem s = new SelectItem(_column, fromItem, _function, _functionParameters, _expression,
                subQuerySelectItem, _alias, _functionApproximationAllowed);
        return s;
    }

    /**
     * Creates a copy of the {@link SelectItem}, with a different
     * {@link FunctionType}.
     * 
     * @param function
     * @return
     */
    public SelectItem replaceFunction(FunctionType function) {
        return new SelectItem(_column, _fromItem, function, _functionParameters, _expression, _subQuerySelectItem,
                _alias, _functionApproximationAllowed);
    }

    /**
     * Creates a copy of the {@link SelectItem}, with a different
     * {@link #isFunctionApproximationAllowed()} flag set.
     * 
     * @param functionApproximationAllowed
     * @return
     */
    public SelectItem replaceFunctionApproximationAllowed(boolean functionApproximationAllowed) {
        return new SelectItem(_column, _fromItem, _function, _functionParameters, _expression, _subQuerySelectItem,
                _alias, functionApproximationAllowed);
    }

    /**
     * Investigates whether or not this SelectItem references a particular
     * column. This will search for direct references and indirect references
     * via subqueries.
     * 
     * @param column
     * @return a boolean that is true if the specified column is referenced by
     *         this SelectItem and false otherwise.
     */
    public boolean isReferenced(Column column) {
        if (column != null) {
            if (column.equals(_column)) {
                return true;
            }
            if (_subQuerySelectItem != null) {
                return _subQuerySelectItem.isReferenced(column);
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
