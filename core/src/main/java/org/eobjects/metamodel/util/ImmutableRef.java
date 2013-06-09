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

/**
 * Simple/hard implementation of the {@link Ref} interface.
 * 
 * @author Kasper Sørensen
 * 
 * @param <E>
 */
public final class ImmutableRef<E> implements Ref<E> {

	private final E _object;

	public ImmutableRef(E object) {
		_object = object;
	}

	@Override
	public E get() {
		return _object;
	}

	public static <E> Ref<E> of(E object) {
		return new ImmutableRef<E>(object);
	}

}
