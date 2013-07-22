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

import static org.apache.metamodel.schema.SuperColumnType.BINARY_TYPE;
import static org.apache.metamodel.schema.SuperColumnType.BOOLEAN_TYPE;
import static org.apache.metamodel.schema.SuperColumnType.LITERAL_TYPE;
import static org.apache.metamodel.schema.SuperColumnType.NUMBER_TYPE;
import static org.apache.metamodel.schema.SuperColumnType.OTHER_TYPE;
import static org.apache.metamodel.schema.SuperColumnType.TIME_TYPE;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.util.NumberComparator;
import org.apache.metamodel.util.ObjectComparator;
import org.apache.metamodel.util.TimeComparator;
import org.apache.metamodel.util.ToStringComparator;

/**
 * Represents the data-type of columns. Most of the elements in this enum are
 * based on the JDBC {@link Types} class, but with a few additions.
 */
public enum ColumnType {

    /**
     * Literal
     */
    CHAR(LITERAL_TYPE), VARCHAR(LITERAL_TYPE), LONGVARCHAR(LITERAL_TYPE), CLOB(LITERAL_TYPE), NCHAR(LITERAL_TYPE), NVARCHAR(
            LITERAL_TYPE), LONGNVARCHAR(LITERAL_TYPE), NCLOB(LITERAL_TYPE),

    /**
     * Numbers
     */
    TINYINT(NUMBER_TYPE), SMALLINT(NUMBER_TYPE), INTEGER(NUMBER_TYPE), BIGINT(NUMBER_TYPE), FLOAT(NUMBER_TYPE), REAL(
            NUMBER_TYPE), DOUBLE(NUMBER_TYPE), NUMERIC(NUMBER_TYPE), DECIMAL(NUMBER_TYPE),

    /**
     * Time based
     */
    DATE(TIME_TYPE), TIME(TIME_TYPE), TIMESTAMP(TIME_TYPE),

    /**
     * Booleans
     */
    BIT(BOOLEAN_TYPE), BOOLEAN(BOOLEAN_TYPE),

    /**
     * Binary types
     */
    BINARY(BINARY_TYPE), VARBINARY(BINARY_TYPE), LONGVARBINARY(BINARY_TYPE), BLOB(BINARY_TYPE),

    /**
     * Other types (as defined in {@link Types}).
     */
    NULL(OTHER_TYPE), OTHER(OTHER_TYPE), JAVA_OBJECT(OTHER_TYPE), DISTINCT(OTHER_TYPE), STRUCT(OTHER_TYPE), ARRAY(
            OTHER_TYPE), REF(OTHER_TYPE), DATALINK(OTHER_TYPE), ROWID(OTHER_TYPE), SQLXML(OTHER_TYPE),

    /**
     * Additional types (added by MetaModel for non-JDBC datastores)
     */
    LIST(OTHER_TYPE), MAP(OTHER_TYPE);

    private SuperColumnType _superType;

    private ColumnType(SuperColumnType superType) {
        if (superType == null) {
            throw new IllegalArgumentException("SuperColumnType cannot be null");
        }
        _superType = superType;
    }

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

    public boolean isBoolean() {
        return _superType == BOOLEAN_TYPE;
    }

    public boolean isBinary() {
        return _superType == BINARY_TYPE;
    }

    public boolean isNumber() {
        return _superType == NUMBER_TYPE;
    }

    public boolean isTimeBased() {
        return _superType == TIME_TYPE;
    }

    public boolean isLiteral() {
        return _superType == LITERAL_TYPE;
    }

    public boolean isLargeObject() {
        switch (this) {
        case BLOB:
        case CLOB:
        case NCLOB:
            return true;
        default:
            return false;
        }
    }

    /**
     * @return a java class that is appropriate for handling column values of
     *         this column type
     */
    public Class<?> getJavaEquivalentClass() {
        switch (this) {
        case TINYINT:
        case SMALLINT:
            return Short.class;
        case INTEGER:
            return Integer.class;
        case BIGINT:
            return BigInteger.class;
        case DECIMAL:
        case NUMERIC:
        case FLOAT:
        case REAL:
        case DOUBLE:
            return Double.class;
        case DATE:
        case TIME:
        case TIMESTAMP:
            return Date.class;
        case BLOB:
            return Blob.class;
        case CLOB:
        case NCLOB:
            return Clob.class;
        case MAP:
            return Map.class;
        case LIST:
            return List.class;
        default:
            // All other types have fitting java equivalent classes in the super
            // type
            return _superType.getJavaEquivalentClass();
        }
    }

    public SuperColumnType getSuperType() {
        return _superType;
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
                    ColumnType[] enumConstants = ColumnType.class.getEnumConstants();
                    for (int j = 0; j < enumConstants.length; j++) {
                        ColumnType columnType = enumConstants[j];
                        if (fieldName.equals(columnType.toString())) {
                            return columnType;
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not access fields in JdbcTypes", e);
        }
        return OTHER;
    }

    /**
     * Gets the JDBC type as per the {@link Types} class.
     * 
     * @return an int representing one of the constants in the {@link Types}
     *         class.
     * @throws IllegalStateException
     *             in case getting the JDBC type was unsuccesful.
     */
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
            type = ColumnType.VARCHAR;
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
        } else if (Map.class.isAssignableFrom(cls)) {
            type = ColumnType.MAP;
        } else if (List.class.isAssignableFrom(cls)) {
            type = ColumnType.LIST;
        } else if (cls == java.sql.Date.class) {
            type = ColumnType.DATE;
        } else if (cls == Timestamp.class) {
            type = ColumnType.TIMESTAMP;
        } else if (cls == Time.class) {
            type = ColumnType.TIME;
        } else if (Date.class.isAssignableFrom(cls)) {
            type = ColumnType.TIMESTAMP;
        } else {
            type = ColumnType.OTHER;
        }
        return type;
    }
}