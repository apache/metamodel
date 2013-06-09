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

import org.eobjects.metamodel.schema.Column;

/**
 * Interface for builder callbacks that "respond" to filter condition building.
 * 
 * @param <B>
 *            the builder type to return once filter has been created.
 */
public interface FilterBuilder<B> {

    /**
     * Not null
     */
    public B isNull();

    /**
     * Is not null
     */
    public B isNotNull();

    /**
     * In ...
     */
    public B in(Collection<?> values);

    /**
     * In ...
     */
    public B in(Number... numbers);

    /**
     * In ...
     */
    public B in(String... strings);

    /**
     * Like ...
     * 
     * (use '%' as wildcard).
     */
    public B like(String string);

    /**
     * Equal to ...
     */
    public B eq(Column column);

    /**
     * Equal to ...
     */
    public B eq(Date date);

    /**
     * Equal to ...
     */
    public B eq(Number number);

    /**
     * Equal to ...
     */
    public B eq(String string);

    /**
     * Equal to ...
     */
    public B eq(Boolean bool);
    
    /**
     * Equal to ...
     */
    public B eq(Object obj);

    /**
     * Equal to ...
     */
    public B isEquals(Column column);

    /**
     * Equal to ...
     */
    public B isEquals(Date date);

    /**
     * Equal to ...
     */
    public B isEquals(Number number);

    /**
     * Equal to ...
     */
    public B isEquals(String string);

    /**
     * Equal to ...
     */
    public B isEquals(Boolean bool);
    
    /**
     * Equal to ...
     */
    public B isEquals(Object obj);

    /**
     * Equal to ...
     * 
     * @deprecated use 'eq' or 'isEquals' instead.
     */
    @Deprecated
    public B equals(Column column);

    /**
     * Equal to ...
     * 
     * @deprecated use 'eq' or 'isEquals' instead.
     */
    @Deprecated
    public B equals(Date date);

    /**
     * Equal to ...
     * 
     * @deprecated use 'eq' or 'isEquals' instead.
     */
    @Deprecated
    public B equals(Number number);

    /**
     * Equal to ...
     * 
     * @deprecated use 'eq' or 'isEquals' instead.
     */
    @Deprecated
    public B equals(String string);

    /**
     * Equal to ...
     * 
     * @deprecated use 'eq' or 'isEquals' instead.
     */
    @Deprecated
    public B equals(Boolean bool);

    /**
     * Not equal to ...
     */
    public B differentFrom(Column column);

    /**
     * Not equal to ...
     */
    public B differentFrom(Date date);

    /**
     * Not equal to ...
     */
    public B differentFrom(Number number);

    /**
     * Not equal to ...
     */
    public B differentFrom(String string);

    /**
     * Not equal to ...
     */
    public B differentFrom(Boolean bool);
    
    /**
     * Not equal to ...
     */
    public B differentFrom(Object obj);

    /**
     * Not equal to ...
     */
    public B ne(Column column);

    /**
     * Not equal to ...
     */
    public B ne(Date date);

    /**
     * Not equal to ...
     */
    public B ne(Number number);

    /**
     * Not equal to ...
     */
    public B ne(String string);

    /**
     * Not equal to ...
     */
    public B ne(Boolean bool);
    
    /**
     * Not equal to ...
     */
    public B ne(Object obj);

    /**
     * Greater than ...
     * 
     * @deprecated use {@link #greaterThan(Column)} instead
     */
    @Deprecated
    public B higherThan(Column column);

    /**
     * Greater than ...
     */
    public B greaterThan(Column column);

    /**
     * Greater than ...
     */
    public B gt(Column column);
    
    /**
     * Greater than ...
     */
    public B greaterThan(Object obj);
    
    /**
     * Greater than ...
     */
    public B gt(Object obj);

    /**
     * Greater than ...
     * 
     * @deprecated use {@link #greaterThan(Date)} instead
     */
    @Deprecated
    public B higherThan(Date date);

    /**
     * Greater than ...
     */
    public B greaterThan(Date date);

    /**
     * Greater than ...
     */
    public B gt(Date date);

    /**
     * @deprecated use {@link #greaterThan(Number)} instead
     */
    @Deprecated
    public B higherThan(Number number);

    /**
     * Greater than ...
     */
    public B greaterThan(Number number);

    /**
     * Greater than ...
     */
    public B gt(Number number);

    /**
     * Greater than ...
     * 
     * @deprecated use {@link #greaterThan(String)} instead
     */
    @Deprecated
    public B higherThan(String string);

    /**
     * Greater than ...
     */
    public B greaterThan(String string);

    /**
     * Greater than ...
     */
    public B gt(String string);

    /**
     * Less than ...
     */
    public B lessThan(Column column);

    /**
     * Less than ...
     */
    public B lt(Column column);

    /**
     * Less than ...
     */
    public B lessThan(Date date);

    /**
     * Less than ...
     */
    public B lessThan(Number number);

    /**
     * Less than ...
     */
    public B lessThan(String string);
    
    /**
     * Less than ...
     */
    public B lessThan(Object obj);
    
    /**
     * Less than ...
     */
    public B lt(Object obj);

    /**
     * Less than ...
     */
    public B lt(Date date);

    /**
     * Less than ...
     */
    public B lt(Number number);

    /**
     * Less than ...
     */
    public B lt(String string);
}
