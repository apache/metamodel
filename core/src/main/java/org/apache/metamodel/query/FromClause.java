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

import org.apache.metamodel.schema.Table;

/**
 * Represents the FROM clause of a query containing FromItem's.
 * 
 * @see FromItem
 */
public class FromClause extends AbstractQueryClause<FromItem> {

    private static final long serialVersionUID = -8227310702249122115L;

    public FromClause(Query query) {
        super(query, AbstractQueryClause.PREFIX_FROM, AbstractQueryClause.DELIM_COMMA);
    }

    /**
     * Gets the alias of a table, if it is registered (and visible, ie. not part
     * of a sub-query) in the FromClause
     * 
     * @param table
     *            the table to get the alias for
     * @return the alias or null if none is found
     */
    public String getAlias(Table table) {
        if (table != null) {
            for (FromItem item : getItems()) {
                String alias = item.getAlias(table);
                if (alias != null) {
                    return alias;
                }
            }
        }
        return null;
    }

    /**
     * Retrieves a table by it's reference which may be it's alias or it's
     * qualified table name. Typically, this method is used to resolve a
     * SelectItem with a reference like "foo.bar", where "foo" may either be an
     * alias or a table name
     * 
     * @param reference
     * @return a FromItem which matches the provided reference string
     */
    public FromItem getItemByReference(String reference) {
        if (reference == null) {
            return null;
        }
        for (final FromItem item : getItems()) {
            FromItem result = getItemByReference(item, reference);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    private FromItem getItemByReference(final FromItem item, final String reference) {
        if (reference.equals(item.toStringNoAlias(false))) {
            return item;
        }
        if (reference.equals(item.toStringNoAlias(true))) {
            return item;
        }
        final String alias = item.getAlias();
        if (reference.equals(alias)) {
            return item;
        }

        final Table table = item.getTable();
        if (alias == null && table != null && reference.equals(table.getName())) {
            return item;
        }

        final JoinType join = item.getJoin();
        if (join != null) {
            final FromItem leftResult = getItemByReference(item.getLeftSide(), reference);
            if (leftResult != null) {
                return leftResult;
            }
            final FromItem rightResult = getItemByReference(item.getRightSide(), reference);
            if (rightResult != null) {
                return rightResult;
            }
        }

        return null;
    }
}