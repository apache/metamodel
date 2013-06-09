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

package org.eobjects.metamodel.query;

import org.eobjects.metamodel.schema.Table;

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

    private FromItem getItemByReference(FromItem item, String reference) {
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