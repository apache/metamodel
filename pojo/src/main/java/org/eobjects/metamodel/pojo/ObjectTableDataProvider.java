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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.util.SimpleTableDef;

/**
 * {@link TableDataProvider} for regular Java objects with getter and setter
 * methods. Each of these method pairs will be treated as fields in a table.
 * 
 * @param <E>
 */
public final class ObjectTableDataProvider<E> implements TableDataProvider<E> {

    private static final long serialVersionUID = 1L;

    private final String _tableName;
    private final Collection<E> _collection;
    private final Class<E> _class;
    private final SimpleTableDef _tableDef;
    private final Map<String, Class<?>> _fieldTypes;

    public ObjectTableDataProvider(Class<E> cls) {
        this(cls.getSimpleName(), cls);
    }

    public ObjectTableDataProvider(String tableName, Class<E> cls) {
        this(tableName, cls, new ArrayList<E>());
    }

    public ObjectTableDataProvider(String tableName, Class<E> cls, Collection<E> collection) {
        _tableName = tableName;
        _collection = collection;
        _class = cls;
        _fieldTypes = new HashMap<String, Class<?>>();
        _tableDef = createTableDef();
    }

    @Override
    public String getName() {
        return _tableName;
    }

    @Override
    public Iterator<E> iterator() {
        return _collection.iterator();
    }

    @Override
    public SimpleTableDef getTableDef() {
        return _tableDef;
    }

    private SimpleTableDef createTableDef() {
        final List<String> columnNames = new ArrayList<String>();
        final List<ColumnType> columnTypes = new ArrayList<ColumnType>();

        final Method[] methods = _class.getMethods();
        for (final Method method : methods) {
            final String methodName = method.getName();
            if (methodName.startsWith("get") && !"getClass".equals(methodName)) {
                if (method.getParameterTypes().length == 0) {
                    final Class<?> returnType = method.getReturnType();
                    if (returnType != null) {
                        final String columnName = Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);

                        _fieldTypes.put(columnName, returnType);
                        columnNames.add(columnName);
                        columnTypes.add(ColumnType.convertColumnType(returnType));
                    }
                }
            }
        }

        final int size = columnNames.size();

        return new SimpleTableDef(_tableName, columnNames.toArray(new String[size]),
                columnTypes.toArray(new ColumnType[size]));
    }

    @Override
    public Object getValue(final String column, E record) {
        final Method getterMethod = getMethod(column, "get");

        try {
            final Object result = getterMethod.invoke(record);
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to invoke getter method: " + getterMethod, e);
        }

    }

    @Override
    public void insert(Map<String, Object> recordData) {
        final E object;
        try {
            object = _class.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to instantiate a new instance of " + _class, e);
        }

        final Set<Entry<String, Object>> entrySet = recordData.entrySet();
        for (Entry<String, Object> entry : entrySet) {
            final String column = entry.getKey();
            final Class<?> fieldType = _fieldTypes.get(column);
            final Method setterMethod = getMethod(column, "set", fieldType);
            final Object value = entry.getValue();

            try {
                setterMethod.invoke(object, value);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to invoke setter method: " + setterMethod, e);
            }
        }

        _collection.add(object);
    }

    private Method getMethod(final String column, final String prefix, final Class<?>... parameterTypes) {
        Method getterMethod;
        try {
            getterMethod = _class.getMethod(prefix + Character.toUpperCase(column.charAt(0)) + column.substring(1),
                    parameterTypes);
        } catch (Exception e) {
            try {
                getterMethod = _class.getMethod(prefix + column, parameterTypes);
            } catch (Exception e2) {
                throw new IllegalArgumentException("Could not find '" + prefix + "' method for column: " + column);
            }
        }
        return getterMethod;
    }

}
