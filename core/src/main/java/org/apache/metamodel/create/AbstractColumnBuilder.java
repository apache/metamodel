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
package org.apache.metamodel.create;

import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;

/**
 * Convenience implementation of all {@link ColumnBuilder} methods
 * 
 * @param <T>
 *            the return type of the builder methods.
 */
abstract class AbstractColumnBuilder<T extends ColumnBuilder<?>> implements ColumnBuilder<T> {

    private final MutableColumn _column;

    public AbstractColumnBuilder(MutableColumn column) {
        _column = column;
    }
    
    protected MutableColumn getColumn() {
        return _column;
    }

    @SuppressWarnings("unchecked")
    protected T getReturnObject() {
        return (T) this;
    }

    @Override
    public final T like(Column column) {
        _column.setColumnSize(column.getColumnSize());
        _column.setNativeType(column.getNativeType());
        _column.setType(column.getType());
        _column.setNullable(column.isNullable());
        _column.setPrimaryKey(column.isPrimaryKey());
        return getReturnObject();
    }

    @Override
    public final T ofType(ColumnType type) {
        _column.setType(type);
        return getReturnObject();
    }

    @Override
    public final T ofNativeType(String nativeType) {
        _column.setNativeType(nativeType);
        return getReturnObject();
    }

    @Override
    public final T ofSize(int size) {
        _column.setColumnSize(size);
        return getReturnObject();
    }

    @Override
    public final T nullable(boolean nullable) {
        _column.setNullable(nullable);
        return getReturnObject();
    }

    @Override
    public final T asPrimaryKey() {
        _column.setPrimaryKey(true);
        return getReturnObject();
    }

}
