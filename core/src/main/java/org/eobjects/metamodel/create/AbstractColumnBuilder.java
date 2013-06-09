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
package org.eobjects.metamodel.create;

import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;

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
