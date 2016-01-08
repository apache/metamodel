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
package org.apache.metamodel.schema;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.metamodel.util.NumberComparator;
import org.apache.metamodel.util.ObjectComparator;
import org.apache.metamodel.util.TimeComparator;
import org.apache.metamodel.util.ToStringComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of ColumnType
 */
public class ColumnTypeImpl implements ColumnType {

    private static final long serialVersionUID = 1L;

    public static final Logger logger = LoggerFactory.getLogger(ColumnTypeImpl.class);

    private final String _name;
    private final SuperColumnType _superColumnType;
    private final Class<?> _javaType;
    private final boolean _largeObject;

    public ColumnTypeImpl(String name, SuperColumnType superColumnType) {
        this(name, superColumnType, null);
    }

    public ColumnTypeImpl(String name, SuperColumnType superColumnType, Class<?> javaType) {
        this(name, superColumnType, javaType, false);
    }

    public ColumnTypeImpl(String name, SuperColumnType superColumnType, Class<?> javaType, boolean largeObject) {
        if (name == null) {
            throw new IllegalArgumentException("Name cannot be null");
        }
        if (superColumnType == null) {
            throw new IllegalArgumentException("SuperColumnType cannot be null");
        }
        _name = name;
        _superColumnType = superColumnType;
        if (javaType == null) {
            _javaType = superColumnType.getJavaEquivalentClass();
        } else {
            _javaType = javaType;
        }
        _largeObject = largeObject;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Comparator<Object> getComparator() {
        if (isTimeBased()) {
            return TimeComparator.getComparator();
        }
        if (isNumber()) {
            return NumberComparator.getComparator();
        }
        if (isLiteral()) {
            return ToStringComparator.getComparator();
        }
        return ObjectComparator.getComparator();
    }

    @Override
    public boolean isBoolean() {
        return _superColumnType == SuperColumnType.BOOLEAN_TYPE;
    }

    @Override
    public boolean isBinary() {
        return _superColumnType == SuperColumnType.BINARY_TYPE;
    }

    @Override
    public boolean isNumber() {
        return _superColumnType == SuperColumnType.NUMBER_TYPE;
    }

    @Override
    public boolean isTimeBased() {
        return _superColumnType == SuperColumnType.TIME_TYPE;
    }

    @Override
    public boolean isLiteral() {
        return _superColumnType == SuperColumnType.LITERAL_TYPE;
    }

    @Override
    public boolean isLargeObject() {
        return _largeObject;
    }

    @Override
    public Class<?> getJavaEquivalentClass() {
        return _javaType;
    }

    @Override
    public SuperColumnType getSuperType() {
        return _superColumnType;
    }

    @Override
    public int getJdbcType() throws IllegalStateException {
        final String name = this.toString();
        try {
            // We assume that the JdbcTypes class only consists of constant
            // integer types, so we make no assertions here
            final Field[] fields = JdbcTypes.class.getFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                String fieldName = field.getName();
                if (fieldName.equals(name)) {
                    int value = (Integer) field.getInt(null);
                    return value;
                }
            }
            throw new IllegalStateException("No JdbcType found with field name: " + name);
        } catch (Exception e) {
            throw new IllegalStateException("Could not access fields in JdbcTypes", e);
        }
    }

    @Override
    public String toString() {
        return _name;
    }

    /**
     * Finds the ColumnType enum corresponding to the incoming JDBC
     * type-constant
     */
    public static ColumnType convertColumnType(int jdbcType) {
        try {
            Field[] fields = JdbcTypes.class.getFields();
            // We assume that the JdbcTypes class only consists of constant
            // integer types, so we make no assertions here
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                int value = (Integer) field.getInt(null);
                if (value == jdbcType) {
                    String fieldName = field.getName();
                    return valueOf(fieldName);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not access fields in JdbcTypes", e);
        }
        return OTHER;
    }

    /**
     * Finds the ColumnType enum corresponding to the incoming Java class.
     * 
     * @param cls
     * @return
     */
    public static ColumnType convertColumnType(Class<?> cls) {
        if (cls == null) {
            throw new IllegalArgumentException("Class cannot be null");
        }

        final ColumnType type;
        if (cls == String.class) {
            type = ColumnType.STRING;
        } else if (cls == Boolean.class || cls == boolean.class) {
            type = ColumnType.BOOLEAN;
        } else if (cls == Character.class || cls == char.class || cls == Character[].class || cls == char[].class) {
            type = ColumnType.CHAR;
        } else if (cls == Byte.class || cls == byte.class) {
            type = ColumnType.TINYINT;
        } else if (cls == Short.class || cls == short.class) {
            type = ColumnType.SMALLINT;
        } else if (cls == Integer.class || cls == int.class) {
            type = ColumnType.INTEGER;
        } else if (cls == Long.class || cls == long.class || cls == BigInteger.class) {
            type = ColumnType.BIGINT;
        } else if (cls == Float.class || cls == float.class) {
            type = ColumnType.FLOAT;
        } else if (cls == Double.class || cls == double.class) {
            type = ColumnType.DOUBLE;
        } else if (cls == BigDecimal.class) {
            type = ColumnType.DECIMAL;
        } else if (Number.class.isAssignableFrom(cls)) {
            type = ColumnType.NUMBER;
        } else if (Map.class.isAssignableFrom(cls)) {
            type = ColumnType.MAP;
        } else if (List.class.isAssignableFrom(cls)) {
            type = ColumnType.LIST;
        } else if (Set.class.isAssignableFrom(cls)) {
            type = ColumnType.SET;
        } else if (cls == java.sql.Date.class) {
            type = ColumnType.DATE;
        } else if (cls == Timestamp.class) {
            type = ColumnType.TIMESTAMP;
        } else if (cls == Time.class) {
            type = ColumnType.TIME;
        } else if (Date.class.isAssignableFrom(cls)) {
            type = ColumnType.TIMESTAMP;
        } else if (cls == UUID.class) {
            type = ColumnType.UUID;
        } else if (cls == InetAddress.class) {
            type = ColumnType.INET;
        } else {
            type = ColumnType.OTHER;
        }
        return type;
    }

    public static ColumnType valueOf(String fieldName) {
        try {
            Field columnTypeField = ColumnType.class.getField(fieldName);
            if (columnTypeField != null) {
                columnTypeField.setAccessible(true);
                Object columnType = columnTypeField.get(ColumnType.class);
                return (ColumnType) columnType;
            }
        } catch (Exception e) {
            logger.error("Failed to resolve JDBC type in ColumnType constants: " + fieldName, e);
        }
        return null;
    }
}
