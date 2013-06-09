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
 * A function that always returns the same constant response.
 * 
 * @param <I>
 * @param <O>
 */
public final class ConstantFunc<I, O> implements Func<I, O> {

    private final O _response;

    public ConstantFunc(O response) {
        _response = response;
    }

    @Override
    public O eval(I arg) {
        return _response;
    }

    @Override
    public int hashCode() {
        if (_response == null) {
            return -1;
        }
        return _response.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof ConstantFunc) {
            Object otherResponse = ((ConstantFunc<?, ?>) obj)._response;
            if (otherResponse == null && _response == null) {
                return true;
            } else if (_response == null) {
                return false;
            } else {
                return _response.equals(otherResponse);
            }
        }
        return false;
    }
}
