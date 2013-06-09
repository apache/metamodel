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
package org.eobjects.metamodel.util;

import java.io.Serializable;

/**
 * {@link Func} useful for mapping {@link HasName} instances to names, using
 * {@link CollectionUtils#map(Object[], Func)} and
 * {@link CollectionUtils#map(Iterable, Func)}.
 * 
 * @author Kasper Sørensen
 */
public final class HasNameMapper implements Func<HasName, String>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
	public String eval(HasName arg) {
		return arg.getName();
	}

}
