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
package org.eobjects.metamodel.drop;

import org.eobjects.metamodel.schema.Table;

/**
 * Abstract {@link TableDropBuilder} implementation
 */
public abstract class AbstractTableDropBuilder implements TableDropBuilder {

    private final Table _table;

    public AbstractTableDropBuilder(Table table) {
        if (table == null) {
            throw new IllegalArgumentException("Table cannot be null");
        }
        _table = table;
    }

    @Override
    public final Table getTable() {
        return _table;
    }
    
    @Override
    public String toString() {
        return toSql();
    }
    
    @Override
    public String toSql() {
        return "DROP TABLE " + _table.getQualifiedLabel();
    }
}
