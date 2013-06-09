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
package org.eobjects.metamodel.pojo;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eobjects.metamodel.util.SimpleTableDef;

/**
 * {@link TableDataProvider} based on an {@link Collection} (for instance a
 * {@link List}) of object arrays.
 */
public class ArrayTableDataProvider implements TableDataProvider<Object[]> {

    private static final long serialVersionUID = 1L;
    private final SimpleTableDef _tableDef;
    private final Collection<Object[]> _arrays;

    public ArrayTableDataProvider(SimpleTableDef tableDef, Collection<Object[]> arrays) {
        _tableDef = tableDef;
        _arrays = arrays;
    }

    @Override
    public String getName() {
        return getTableDef().getName();
    }

    @Override
    public Iterator<Object[]> iterator() {
        return _arrays.iterator();
    }

    @Override
    public SimpleTableDef getTableDef() {
        return _tableDef;
    }

    @Override
    public Object getValue(String columnName, Object[] record) {
        int index = _tableDef.indexOf(columnName);
        return record[index];
    }

    @Override
    public void insert(Map<String, Object> recordData) {
        String[] columnNames = _tableDef.getColumnNames();
        Object[] record = new Object[columnNames.length];
        for (int i = 0; i < record.length; i++) {
            record[i] = recordData.get(columnNames[i]);
        }
        _arrays.add(record);
    }

}
