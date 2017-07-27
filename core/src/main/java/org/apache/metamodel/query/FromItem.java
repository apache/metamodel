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

import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.BaseObject;

/**
 * Represents a FROM item. FROM items can take different forms:
 * <ul>
 * <li>table FROMs (eg. "FROM products p")</li>
 * <lI>join FROMs with an ON clause (eg. "FROM products p INNER JOIN orders o ON
 * p.id = o.product_id")</li>
 * <li>subquery FROMs (eg. "FROM (SELECT * FROM products) p")</li>
 * <li>expression FROM (any string based from item)</li>
 * </ul>
 * 
 * @see FromClause
 */
public class FromItem extends BaseObject implements QueryItem, Cloneable {

    private static final long serialVersionUID = -6559220014058975193L;
    private Table _table;
    private String _alias;
    private Query _subQuery;
    private JoinType _join;
    private FromItem _leftSide;
    private FromItem _rightSide;
    private SelectItem[] _leftOn;
    private SelectItem[] _rightOn;
    private Query _query;
    private String _expression;

    /**
     * Private constructor, used for cloning
     */
    private FromItem() {
    }

    /**
     * Constructor for table FROM clauses
     */
    public FromItem(Table table) {
        _table = table;
    }

    /**
     * Constructor for sub-query FROM clauses
     * 
     * @param subQuery
     *            the subquery to use
     */
    public FromItem(Query subQuery) {
        _subQuery = subQuery;
    }

    /**
     * Constructor for join FROM clauses that join two tables using their
     * relationship. The primary table of the relationship will be the left side
     * of the join and the foreign table of the relationship will be the right
     * side of the join.
     * 
     * @param join
     *            the join type to use
     * @param relationship
     *            the relationship to use for joining the tables
     */
    public FromItem(JoinType join, Relationship relationship) {
        _join = join;
        _leftSide = new FromItem(relationship.getPrimaryTable());
        List<Column> columns = relationship.getPrimaryColumns();
        _leftOn = new SelectItem[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            _leftOn[i] = new SelectItem(columns.get(i));
        }
        _rightSide = new FromItem(relationship.getForeignTable());
        columns = relationship.getForeignColumns();
        _rightOn = new SelectItem[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            _rightOn[i] = new SelectItem(columns.get(i));
        }
    }

    /**
     * Constructor for advanced join types with custom relationships
     * 
     * @param join
     *            the join type to use
     * @param leftSide
     *            the left side of the join
     * @param rightSide
     *            the right side of the join
     * @param leftOn
     *            what left-side select items to use for the ON clause
     * @param rightOn
     *            what right-side select items to use for the ON clause
     */
    public FromItem(JoinType join, FromItem leftSide, FromItem rightSide, SelectItem[] leftOn, SelectItem[] rightOn) {
        _join = join;
        _leftSide = leftSide;
        _rightSide = rightSide;
        _leftOn = leftOn;
        _rightOn = rightOn;
    }

    /**
     * Creates a single unvalidated from item based on a expression. Expression
     * based from items are typically NOT datastore-neutral but are available
     * for special "hacking" needs.
     * 
     * Expression based from items can only be used for JDBC based datastores
     * since they are translated directly into SQL.
     * 
     * @param expression
     *            An expression to use for the from item, for example "MYTABLE".
     */
    public FromItem(String expression) {
        if (expression == null) {
            throw new IllegalArgumentException("Expression cannot be null");
        }
        _expression = expression;
    }

    public String getAlias() {
        return _alias;
    }

    public String getSameQueryAlias() {
        if (_alias != null) {
            return _alias;
        }
        if (_table != null) {
            return _table.getQuotedName();
        }
        return null;
    }

    public FromItem setAlias(String alias) {
        _alias = alias;
        return this;
    }

    public Table getTable() {
        return _table;
    }

    public Query getSubQuery() {
        return _subQuery;
    }

    public JoinType getJoin() {
        return _join;
    }

    public FromItem getLeftSide() {
        return _leftSide;
    }

    public FromItem getRightSide() {
        return _rightSide;
    }

    public SelectItem[] getLeftOn() {
        return _leftOn;
    }

    public SelectItem[] getRightOn() {
        return _rightOn;
    }

    public String getExpression() {
        return _expression;
    }

    @Override
    public String toSql() {
        return toSql(false);
    }

    @Override
    public String toSql(boolean includeSchemaInColumnPaths) {
        final String stringNoAlias = toStringNoAlias(includeSchemaInColumnPaths);
        final StringBuilder sb = new StringBuilder(stringNoAlias);
        if (_join != null && _alias != null) {
            sb.insert(0, '(');
            sb.append(')');
        }
        if (_alias != null) {
            sb.append(' ');
            sb.append(_alias);
        }
        return sb.toString();
    }

    public String toStringNoAlias() {
        return toStringNoAlias(false);
    }

    public String toStringNoAlias(boolean includeSchemaInColumnPaths) {
        if (_expression != null) {
            return _expression;
        }
        StringBuilder sb = new StringBuilder();
        if (_table != null) {
            if (_table.getSchema() != null && _table.getSchema().getName() != null) {
                sb.append(_table.getSchema().getName());
                sb.append('.');
            }
            sb.append(_table.getQuotedName());
        } else if (_subQuery != null) {
            sb.append('(');
            sb.append(_subQuery.toSql(includeSchemaInColumnPaths));
            sb.append(')');
        } else if (_join != null) {
            String leftSideAlias = _leftSide.getSameQueryAlias();
            String rightSideAlias = _rightSide.getSameQueryAlias();
            sb.append(_leftSide.toSql());
            sb.append(' ');
            sb.append(_join);
            sb.append(" JOIN ");
            sb.append(_rightSide.toSql());
            for (int i = 0; i < _leftOn.length; i++) {
                if (i == 0) {
                    sb.append(" ON ");
                } else {
                    sb.append(" AND ");
                }
                SelectItem primary = _leftOn[i];
                appendJoinOnItem(sb, leftSideAlias, primary);

                sb.append(" = ");

                SelectItem foreign = _rightOn[i];
                appendJoinOnItem(sb, rightSideAlias, foreign);
            }
        }
        return sb.toString();
    }

    private void appendJoinOnItem(StringBuilder sb, String sideAlias, SelectItem onItem) {
        final FromItem fromItem = onItem.getFromItem();
        if (fromItem != null && fromItem.getSubQuery() != null && fromItem.getAlias() != null) {
            // there's a corner case scenario where an ON item references a
            // subquery being joined. In that case the getSuperQueryAlias()
            // method will include the subquery alias.
            final String superQueryAlias = onItem.getSuperQueryAlias();
            sb.append(superQueryAlias);
            return;
        }
        
        if(_join != null && _leftSide.getJoin() != null) {
            sb.append(onItem.toSql());
            return;
        }

        if (sideAlias != null) {
            sb.append(sideAlias);
            sb.append('.');
        }
        final String superQueryAlias = onItem.getSuperQueryAlias();
        sb.append(superQueryAlias);
    }

    /**
     * Gets the alias of a table, if it is registered (and visible, ie. not part
     * of a sub-query) in the FromItem
     * 
     * @param table
     *            the table to get the alias for
     * @return the alias or null if none is found
     */
    public String getAlias(Table table) {
        String result = null;
        if (table != null) {
            // Search recursively through left and right side, unless they
            // are sub-query FromItems
            if (table.equals(_table)) {
                result = _alias;
            } else if (_join != null) {
                result = _rightSide.getAlias(table);
                if (result == null) {
                    result = _leftSide.getAlias(table);
                }
            }
        }
        return result;
    }

    public Query getQuery() {
        return _query;
    }

    public QueryItem setQuery(Query query) {
        _query = query;
        return this;
    }

    @Override
    protected FromItem clone() {
        FromItem f = new FromItem();
        f._alias = _alias;
        f._join = _join;
        f._table = _table;
        f._expression = _expression;
        if (_subQuery != null) {
            f._subQuery = _subQuery.clone();
        }
        if (_leftOn != null && _leftSide != null && _rightOn != null && _rightSide != null) {
            f._leftSide = _leftSide.clone();
            f._leftOn = _leftOn.clone();
            f._rightSide = _rightSide.clone();
            f._rightOn = _rightOn.clone();
        }
        return f;
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(_table);
        identifiers.add(_alias);
        identifiers.add(_subQuery);
        identifiers.add(_join);
        identifiers.add(_leftSide);
        identifiers.add(_rightSide);
        identifiers.add(_leftOn);
        identifiers.add(_rightOn);
        identifiers.add(_expression);
    }

    @Override
    public String toString() {
        return toSql();
    }
}