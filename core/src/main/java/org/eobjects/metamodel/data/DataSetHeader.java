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
package org.eobjects.metamodel.data;

import java.io.Serializable;

import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

/**
 * Represents the header of a {@link DataSet}, which define the
 * columns/SelectItems of it.
 */
public interface DataSetHeader extends Serializable {

    public SelectItem[] getSelectItems();

    public int size();

    public int indexOf(SelectItem item);

    public int indexOf(Column column);

    public SelectItem getSelectItem(int i);
}
