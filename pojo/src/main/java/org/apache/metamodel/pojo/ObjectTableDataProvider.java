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
package org.apache.metamodel.pojo;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ColumnTypeImpl;
import org.apache.metamodel.util.SimpleTableDef;

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
        final Map<String,ColumnType> columns = new TreeMap<String, ColumnType>();

        final Method[] methods = _class.getMethods();
        for (final Method method : methods) {
            final String methodName = method.getName();
            if (methodName.startsWith("get") && !"getClass".equals(methodName)) {
                if (method.getParameterTypes().length == 0) {
                    final Class<?> returnType = method.getReturnType();
                    if (returnType != null) {
                        final String columnName = Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);

                        _fieldTypes.put(columnName, returnType);
                        final ColumnType columnType = ColumnTypeImpl.convertColumnType(returnType);
                        columns.put(columnName, columnType);
                    }
                }
            }
        }

        final int size = columns.size();
        final String[] columnNames= new String[size];
        final ColumnType[] columnTypes = new ColumnType[size];
        
        final Set<Entry<String, ColumnType>> entrySet = columns.entrySet();
        int i= 0;
        for (Entry<String, ColumnType> entry : entrySet) {
            columnNames[i] = entry.getKey();
            columnTypes[i] = entry.getValue();
            i++;
        }

        return new SimpleTableDef(_tableName, columnNames,
                columnTypes);
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
