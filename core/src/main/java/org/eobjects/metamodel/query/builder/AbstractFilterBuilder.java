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
package org.eobjects.metamodel.query.builder;

import java.util.Collection;
import java.util.Date;

import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.OperatorType;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

/**
 * Abstract implementation of {@link FilterBuilder} interface. All built filters
 * are channeled to the {@link #applyFilter(FilterItem)} method which needs to
 * be implemented by concrete implementations.
 */
public abstract class AbstractFilterBuilder<B> implements FilterBuilder<B> {

    private final SelectItem _selectItem;

    public AbstractFilterBuilder(SelectItem selectItem) {
        this._selectItem = selectItem;
    }

    protected abstract B applyFilter(FilterItem filter);

    /**
     * Provides a way to
     * 
     * @param queryParameter
     * @return
     */
    public B applyFilter(OperatorType operator, Object operand) {
        return applyFilter(new FilterItem(_selectItem, operator, operand));
    }

    @Override
    public B in(Collection<?> values) {
        return applyFilter(new FilterItem(_selectItem, OperatorType.IN, values));
    }

    @Override
    public B in(Number... numbers) {
        return applyFilter(new FilterItem(_selectItem, OperatorType.IN, numbers));
    }

    @Override
    public B in(String... strings) {
        return applyFilter(new FilterItem(_selectItem, OperatorType.IN, strings));
    }

    @Override
    public B isNull() {
        return applyFilter(new FilterItem(_selectItem, OperatorType.EQUALS_TO, null));
    }

    @Override
    public B isNotNull() {
        return applyFilter(new FilterItem(_selectItem, OperatorType.DIFFERENT_FROM, null));
    }

    @Override
    public B isEquals(Column column) {
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.EQUALS_TO, new SelectItem(column)));
    }

    @Override
    public B isEquals(Date date) {
        if (date == null) {
            throw new IllegalArgumentException("date cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.EQUALS_TO, date));
    }

    @Override
    public B isEquals(Number number) {
        if (number == null) {
            throw new IllegalArgumentException("number cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.EQUALS_TO, number));
    }

    @Override
    public B isEquals(String string) {
        if (string == null) {
            throw new IllegalArgumentException("string cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.EQUALS_TO, string));
    }

    @Override
    public B isEquals(Boolean bool) {
        if (bool == null) {
            throw new IllegalArgumentException("bool cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.EQUALS_TO, bool));
    }

    @Override
    public B isEquals(Object obj) {
        if (obj == null) {
            return isNull();
        }
        if (obj instanceof Boolean) {
            return isEquals((Boolean) obj);
        }
        if (obj instanceof Number) {
            return isEquals((Number) obj);
        }
        if (obj instanceof Date) {
            return isEquals((Date) obj);
        }
        if (obj instanceof String) {
            return isEquals((String) obj);
        }
        throw new UnsupportedOperationException("Argument must be a Boolean, Number, Date or String. Found: " + obj);
    }

    @Override
    public B differentFrom(Column column) {
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.DIFFERENT_FROM, new SelectItem(column)));
    }

    @Override
    public B differentFrom(Date date) {
        if (date == null) {
            throw new IllegalArgumentException("date cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.DIFFERENT_FROM, date));
    }

    @Override
    public B differentFrom(Number number) {
        if (number == null) {
            throw new IllegalArgumentException("number cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.DIFFERENT_FROM, number));
    }

    @Override
    public B differentFrom(String string) {
        if (string == null) {
            throw new IllegalArgumentException("string cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.DIFFERENT_FROM, string));
    }

    @Override
    public B differentFrom(Boolean bool) {
        if (bool == null) {
            throw new IllegalArgumentException("bool cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.DIFFERENT_FROM, bool));
    }

    @Override
    public B differentFrom(Object obj) {
        if (obj == null) {
            return isNotNull();
        }
        if (obj instanceof Boolean) {
            return differentFrom((Boolean) obj);
        }
        if (obj instanceof Number) {
            return differentFrom((Number) obj);
        }
        if (obj instanceof Date) {
            return differentFrom((Date) obj);
        }
        if (obj instanceof String) {
            return differentFrom((String) obj);
        }
        throw new UnsupportedOperationException("Argument must be a Boolean, Number, Date or String. Found: " + obj);
    }

    @Deprecated
    @Override
    public B higherThan(Column arg) {
        return greaterThan(arg);
    }

    @Override
    public B greaterThan(Column column) {
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.GREATER_THAN, new SelectItem(column)));
    }

    @Deprecated
    @Override
    public B higherThan(Date arg) {
        return greaterThan(arg);
    }

    @Override
    public B greaterThan(Date date) {
        if (date == null) {
            throw new IllegalArgumentException("date cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.GREATER_THAN, date));
    }

    @Deprecated
    @Override
    public B higherThan(Number arg) {
        return greaterThan(arg);
    }

    @Override
    public B greaterThan(Number number) {
        if (number == null) {
            throw new IllegalArgumentException("number cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.GREATER_THAN, number));
    }

    @Deprecated
    @Override
    public B higherThan(String arg) {
        return greaterThan(arg);
    }

    @Override
    public B greaterThan(String string) {
        if (string == null) {
            throw new IllegalArgumentException("string cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.GREATER_THAN, string));
    }

    @Override
    public B lessThan(Column column) {
        if (column == null) {
            throw new IllegalArgumentException("column cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.LESS_THAN, new SelectItem(column)));
    }

    @Override
    public B lessThan(Date date) {
        if (date == null) {
            throw new IllegalArgumentException("date cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.LESS_THAN, date));
    }

    @Override
    public B lessThan(Number number) {
        if (number == null) {
            throw new IllegalArgumentException("number cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.LESS_THAN, number));
    }

    @Override
    public B lessThan(String string) {
        if (string == null) {
            throw new IllegalArgumentException("string cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.LESS_THAN, string));
    }
    
    @Override
    public B lessThan(Object obj) {
        if (obj instanceof Number) {
            return lessThan((Number) obj);
        }
        if (obj instanceof Date) {
            return lessThan((Date) obj);
        }
        if (obj instanceof String) {
            return lessThan((String) obj);
        }
        throw new UnsupportedOperationException("Argument must be a Number, Date or String. Found: " + obj);
    }
    
    @Override
    public B greaterThan(Object obj) {
        if (obj instanceof Number) {
            return greaterThan((Number) obj);
        }
        if (obj instanceof Date) {
            return greaterThan((Date) obj);
        }
        if (obj instanceof String) {
            return greaterThan((String) obj);
        }
        throw new UnsupportedOperationException("Argument must be a Number, Date or String. Found: " + obj);
    }

    @Override
    public B like(String string) {
        if (string == null) {
            throw new IllegalArgumentException("string cannot be null");
        }
        return applyFilter(new FilterItem(_selectItem, OperatorType.LIKE, string));
    }

    @Override
    public B gt(Column column) {
        return greaterThan(column);
    }

    @Override
    public B gt(Date date) {
        return greaterThan(date);
    }

    @Override
    public B gt(Number number) {
        return greaterThan(number);
    }

    @Override
    public B gt(String string) {
        return greaterThan(string);
    }

    @Override
    public B lt(Column column) {
        return lessThan(column);
    }

    public B lt(Date date) {
        return lessThan(date);
    };

    public B lt(Number number) {
        return lessThan(number);
    };

    public B lt(String string) {
        return lessThan(string);
    };

    @Override
    public B eq(Boolean bool) {
        return isEquals(bool);
    }

    @Override
    public B eq(Column column) {
        return isEquals(column);
    }

    @Override
    public B eq(Date date) {
        return isEquals(date);
    }

    @Override
    public B eq(Number number) {
        return isEquals(number);
    }

    @Override
    public B eq(String string) {
        return isEquals(string);
    }

    @Override
    public B eq(Object obj) {
        return isEquals(obj);
    }

    @Override
    public B ne(Boolean bool) {
        return differentFrom(bool);
    }

    @Override
    public B ne(Column column) {
        return differentFrom(column);
    }

    @Override
    public B ne(Date date) {
        return differentFrom(date);
    }

    @Override
    public B ne(Number number) {
        return differentFrom(number);
    }

    @Override
    public B ne(String string) {
        return differentFrom(string);
    }

    @Override
    public B ne(Object obj) {
        return differentFrom(obj);
    }

    @Override
    @Deprecated
    public B equals(Boolean bool) {
        return isEquals(bool);
    }

    @Override
    @Deprecated
    public B equals(Column column) {
        return isEquals(column);
    }

    @Override
    @Deprecated
    public B equals(Date date) {
        return isEquals(date);
    }

    @Override
    @Deprecated
    public B equals(Number number) {
        return isEquals(number);
    }

    @Override
    @Deprecated
    public B equals(String string) {
        return isEquals(string);
    }
    
    @Override
    public B lt(Object obj) {
        return lessThan(obj);
    }
    
    @Override
    public B gt(Object obj) {
        return greaterThan(obj);
    }
}