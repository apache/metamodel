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
package org.eobjects.metamodel.schema;

/**
 * Abstract {@link Column} implementation. Implements most common and trivial
 * methods.
 * 
 * @author Kasper Sørensen
 */
public abstract class AbstractColumn implements Column {

    private static final long serialVersionUID = 1L;

    @Override
    public final String getQuotedName() {
        String quote = getQuote();
        if (quote == null) {
            return getName();
        }
        return quote + getName() + quote;
    }

    @Override
    public final String getQualifiedLabel() {
        StringBuilder sb = new StringBuilder();
        Table table = getTable();
        if (table != null) {
            sb.append(table.getQualifiedLabel());
            sb.append('.');
        }
        sb.append(getName());
        return sb.toString();
    }

    @Override
    public final int compareTo(Column that) {
        int diff = getQualifiedLabel().compareTo(that.getQualifiedLabel());
        if (diff == 0) {
            diff = toString().compareTo(that.toString());
        }
        return diff;
    }

    @Override
    public final String toString() {
        return "Column[name=" + getName() + ",columnNumber=" + getColumnNumber() + ",type=" + getType() + ",nullable="
                + isNullable() + ",nativeType=" + getNativeType() + ",columnSize=" + getColumnSize() + "]";
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof Column) {
            Column other = (Column) obj;
            if (!getName().equals(other.getName())) {
                return false;
            }
            if (getType() != other.getType()) {
                return false;
            }

            final Table table1 = getTable();
            final Table table2 = other.getTable();
            if (table1 == null) {
                if (table2 != null) {
                    return false;
                }
            } else {
                if (!table1.equals(table2)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}