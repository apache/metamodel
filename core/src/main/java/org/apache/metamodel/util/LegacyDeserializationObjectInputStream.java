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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Comparator;

import org.apache.metamodel.query.AggregateFunction;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.SuperColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A specialized {@link ObjectInputStream} for MetaModel which can be used or
 * extended if it is needed to deserialize legacy MetaModel objects. This is
 * needed since the namespace of MetaModel was changed from org.apache.metamodel
 * to org.apache.metamodel.
 */
public class LegacyDeserializationObjectInputStream extends ObjectInputStream {

    private static final Logger logger = LoggerFactory.getLogger(LegacyDeserializationObjectInputStream.class);

    /**
     * Utility method for setting a field in a class
     * 
     * @param cls
     * @param fieldName
     * @param value
     */
    public static void setField(Class<?> cls, Object instance, String fieldName, Object value) {
        try {
            final Field field = cls.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(instance, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Unable to assign field '" + cls.getSimpleName() + '.' + fieldName + "' to value: " + value, e);
        }
    }

    /**
     * Implementation of the new {@link FunctionType} and
     * {@link AggregateFunction} interfaces which still adheres to the
     * constant/enum values of the old FunctionType definition. While
     * deserializing old FunctionType objects, we will convert them to this
     * enum.
     */
    protected static enum LegacyFunctionType implements AggregateFunction {

        COUNT(FunctionType.COUNT), AVG(FunctionType.AVG), SUM(FunctionType.SUM), MAX(FunctionType.MAX), MIN(
                FunctionType.MIN);

        private final AggregateFunction _delegate;

        private LegacyFunctionType(AggregateFunction delegate) {
            _delegate = delegate;
        }

        @Override
        public ColumnType getExpectedColumnType(ColumnType type) {
            return _delegate.getExpectedColumnType(type);
        }

        @Override
        public String getFunctionName() {
            return _delegate.getFunctionName();
        }

        @Override
        public AggregateBuilder<?> createAggregateBuilder() {
            return _delegate.createAggregateBuilder();
        }

        @Override
        public Object evaluate(Object... values) {
            return _delegate.evaluate(values);
        }
    }

    /**
     * Implementation of the new {@link ColumnType} interface which still
     * adheres to the constant/enum values of the old ColumnType definition.
     * While deserializing old ColumnType objects, we will convert them to this
     * enum.
     */
    protected static enum LegacyColumnType implements ColumnType {

        CHAR(ColumnType.CHAR), VARCHAR(ColumnType.VARCHAR), LONGVARCHAR(ColumnType.LONGVARCHAR), CLOB(ColumnType.CLOB), NCHAR(
                ColumnType.NCHAR), NVARCHAR(ColumnType.NVARCHAR), LONGNVARCHAR(ColumnType.LONGNVARCHAR), NCLOB(
                ColumnType.NCLOB), TINYINT(ColumnType.TINYINT), SMALLINT(ColumnType.SMALLINT), INTEGER(
                ColumnType.INTEGER), BIGINT(ColumnType.BIGINT), FLOAT(ColumnType.FLOAT), REAL(ColumnType.REAL), DOUBLE(
                ColumnType.DOUBLE), NUMERIC(ColumnType.NUMERIC), DECIMAL(ColumnType.DECIMAL), DATE(ColumnType.DATE), TIME(
                ColumnType.TIME), TIMESTAMP(ColumnType.TIMESTAMP), BIT(ColumnType.BIT), BOOLEAN(ColumnType.BOOLEAN), BINARY(
                ColumnType.BINARY), VARBINARY(ColumnType.VARBINARY), LONGVARBINARY(ColumnType.LONGVARBINARY), BLOB(
                ColumnType.BLOB), NULL(ColumnType.NULL), OTHER(ColumnType.OTHER), JAVA_OBJECT(ColumnType.JAVA_OBJECT), DISTINCT(
                ColumnType.DISTINCT), STRUCT(ColumnType.STRUCT), ARRAY(ColumnType.ARRAY), REF(ColumnType.REF), DATALINK(
                ColumnType.DATALINK), ROWID(ColumnType.ROWID), SQLXML(ColumnType.SQLXML), LIST(ColumnType.LIST), MAP(
                ColumnType.MAP);

        private final ColumnType _delegate;

        private LegacyColumnType(ColumnType delegate) {
            _delegate = delegate;
        }

        @Override
        public String getName() {
            return _delegate.getName();
        }

        @Override
        public Comparator<Object> getComparator() {
            return _delegate.getComparator();
        }

        @Override
        public boolean isBoolean() {
            return _delegate.isBoolean();
        }

        @Override
        public boolean isBinary() {
            return _delegate.isBinary();
        }

        @Override
        public boolean isNumber() {
            return _delegate.isNumber();
        }

        @Override
        public boolean isTimeBased() {
            return _delegate.isTimeBased();
        }

        @Override
        public boolean isLiteral() {
            return _delegate.isLiteral();
        }

        @Override
        public boolean isLargeObject() {
            return _delegate.isLargeObject();
        }

        @Override
        public Class<?> getJavaEquivalentClass() {
            return _delegate.getJavaEquivalentClass();
        }

        @Override
        public SuperColumnType getSuperType() {
            return _delegate.getSuperType();
        }

        @Override
        public int getJdbcType() throws IllegalStateException {
            return _delegate.getJdbcType();
        }
    }

    /**
     * Implementation of the new {@link OperatorType} interface which still
     * adheres to the constant/enum values of the old OperatorType definition.
     * While deserializing old OperatorType objects, we will convert them to
     * this enum.
     */
    protected static enum LegacyOperatorType implements OperatorType {

        EQUALS_TO(OperatorType.EQUALS_TO), DIFFERENT_FROM(OperatorType.DIFFERENT_FROM), LIKE(OperatorType.LIKE), GREATER_THAN(
                OperatorType.GREATER_THAN), GREATER_THAN_OR_EQUAL(OperatorType.GREATER_THAN_OR_EQUAL), LESS_THAN(
                OperatorType.LESS_THAN), LESS_THAN_OR_EQUAL(OperatorType.LESS_THAN_OR_EQUAL), IN(OperatorType.IN);

        private final OperatorType _delegate;

        private LegacyOperatorType(OperatorType delegate) {
            _delegate = delegate;
        }
        
        @Override
        public boolean isSpaceDelimited() {
            return _delegate.isSpaceDelimited();
        }

        @Override
        public String toSql() {
            return _delegate.toSql();
        }

    }

    private static final String OLD_CLASS_NAME_COLUMN_TYPE = "org.eobjects.metamodel.schema.ColumnType";
    private static final String CLASS_NAME_OPERATOR_TYPE = "org.apache.metamodel.query.OperatorType";
    private static final String CLASS_NAME_FUNCTION_TYPE = "org.apache.metamodel.query.FunctionType";

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
        final String className = objectStreamClass.getName();
        switch (className) {
        case OLD_CLASS_NAME_COLUMN_TYPE:
            final ObjectStreamClass legacyColumnTypeResult = ObjectStreamClass.lookup(LegacyColumnType.class);
            return legacyColumnTypeResult;
        case CLASS_NAME_OPERATOR_TYPE:
            if (isEnumExpected(objectStreamClass)) {
                final ObjectStreamClass legacyOperatorTypeResult = ObjectStreamClass.lookup(LegacyOperatorType.class);
                return legacyOperatorTypeResult;
            }
            break;
        case CLASS_NAME_FUNCTION_TYPE:
            if (isEnumExpected(objectStreamClass)) {
                final ObjectStreamClass legacyOperatorTypeResult = ObjectStreamClass.lookup(LegacyFunctionType.class);
                return legacyOperatorTypeResult;
            }
            break;
        }
        return objectStreamClass;
    }

    /**
     * Method that uses the (non-public) isEnum() method of
     * {@link ObjectStreamClass} to determine if an enum is expected.
     * 
     * @param objectStreamClass
     * @return
     */
    private boolean isEnumExpected(ObjectStreamClass objectStreamClass) {
        try {
            final Field initializedField = ObjectStreamClass.class.getDeclaredField("initialized");
            initializedField.setAccessible(true);
            final Boolean initialized = (Boolean) initializedField.get(objectStreamClass);
            if (!initialized) {
                /*
                 * Snippet from the JDK source:
                 * 
                 * void initNonProxy(ObjectStreamClass model,
                 * Class<?> cl,
                 * ClassNotFoundException resolveEx,
                 * ObjectStreamClass superDesc)
                 **/
                final Method initMethod = ObjectStreamClass.class.getDeclaredMethod("initNonProxy", ObjectStreamClass.class,
                        Class.class, ClassNotFoundException.class, ObjectStreamClass.class);
                initMethod.setAccessible(true);
                initMethod.invoke(objectStreamClass, objectStreamClass, null, null, null);
            }
        } catch (NoSuchFieldError e) {
            logger.debug("Failed to access boolean field 'initialized' in {}", objectStreamClass.getName(), e);
        } catch (Exception e) {
            logger.debug("Failed to access invoke ObjectStreamClass.initialize() to prepare {}", objectStreamClass
                    .getName(), e);
        }
        
        try {
            final Method isEnumMethod = ObjectStreamClass.class.getDeclaredMethod("isEnum");
            isEnumMethod.setAccessible(true);
            final Boolean result = (Boolean) isEnumMethod.invoke(objectStreamClass);
            return result.booleanValue();
        } catch (Exception e) {
            logger.warn("Failed to access and invoke ObjectStreamClass.isEnum to determine if {} is an enum",
                    objectStreamClass.getName(), e);
        }
        return false;
    }
}
