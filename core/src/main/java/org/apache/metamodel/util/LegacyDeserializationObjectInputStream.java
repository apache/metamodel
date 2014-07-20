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
package org.apache.metamodel.util;

import static org.apache.metamodel.schema.SuperColumnType.BINARY_TYPE;
import static org.apache.metamodel.schema.SuperColumnType.BOOLEAN_TYPE;
import static org.apache.metamodel.schema.SuperColumnType.LITERAL_TYPE;
import static org.apache.metamodel.schema.SuperColumnType.NUMBER_TYPE;
import static org.apache.metamodel.schema.SuperColumnType.OTHER_TYPE;
import static org.apache.metamodel.schema.SuperColumnType.TIME_TYPE;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Types;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.JdbcTypes;
import org.apache.metamodel.schema.SuperColumnType;

/**
 * A specialized {@link ObjectInputStream} for MetaModel which can be used or
 * extended if it is needed to deserialize legacy MetaModel objects. This is
 * needed since the namespace of MetaModel was changed from
 * org.apache.metamodel to org.apache.metamodel.
 */
public class LegacyDeserializationObjectInputStream extends ObjectInputStream {

    /**
     * Implementation of the new {@link ColumnType} interface which still
     * adheres to the constant/enum values of the old ColumnType definition.
     * While deserializing old ColumnType objects, we will convert them to this
     * enum.
     */
    protected static enum LegacyColumnType implements ColumnType {

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

        private final SuperColumnType _superType;

        private LegacyColumnType(SuperColumnType superType) {
            if (superType == null) {
                throw new IllegalArgumentException("SuperColumnType cannot be null");
            }
            _superType = superType;
        }

        @Override
        public String getName() {
            return name();
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
            return _superType == BOOLEAN_TYPE;
        }

        @Override
        public boolean isBinary() {
            return _superType == BINARY_TYPE;
        }

        @Override
        public boolean isNumber() {
            return _superType == NUMBER_TYPE;
        }

        @Override
        public boolean isTimeBased() {
            return _superType == TIME_TYPE;
        }

        @Override
        public boolean isLiteral() {
            return _superType == LITERAL_TYPE;
        }

        @Override
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

        @Override
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
                // All other types have fitting java equivalent classes in the
                // super
                // type
                return _superType.getJavaEquivalentClass();
            }
        }

        @Override
        public SuperColumnType getSuperType() {
            return _superType;
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
    }

    private static final String OLD_CLASS_NAME_COLUMN_TYPE = "org.eobjects.metamodel.schema.ColumnType";

    public LegacyDeserializationObjectInputStream(InputStream in) throws IOException, SecurityException {
        super(in);
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        final String className = desc.getName();
        if (className.startsWith("org.eobjects.metamodel") || className.startsWith("[Lorg.eobjects.metamodel")) {
            final String newClassName;
            if (OLD_CLASS_NAME_COLUMN_TYPE.equals(className)) {
                // since ColumnType was changed from enum to interface, there's
                // some special treatment here.
                newClassName = LegacyColumnType.class.getName();
            } else {
                newClassName = className.replace("org.eobjects", "org.apache");
            }
            return Class.forName(newClassName);
        }
        return super.resolveClass(desc);
    }

    @Override
    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
        final ObjectStreamClass objectStreamClass = super.readClassDescriptor();
        if (OLD_CLASS_NAME_COLUMN_TYPE.equals(objectStreamClass.getName())) {
            final ObjectStreamClass result = ObjectStreamClass.lookup(LegacyColumnType.class);
            return result;
        }
        return objectStreamClass;
    }
}
