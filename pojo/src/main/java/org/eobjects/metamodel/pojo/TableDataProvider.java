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

import java.io.Serializable;
import java.util.Map;

import org.eobjects.metamodel.util.HasName;
import org.eobjects.metamodel.util.SimpleTableDef;

/**
 * Represents a named collection to be comprehended by a {@link PojoDataContext}
 * .
 */
public interface TableDataProvider<E> extends HasName, Iterable<E>, Serializable {

    public SimpleTableDef getTableDef();

    public Object getValue(String column, E record);

    public void insert(Map<String, Object> recordData);
}
