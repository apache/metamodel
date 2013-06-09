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
 * A serializable reference to an object which may or may not be serializable.
 * Using this reference there is safety that if the object IS serializable, it
 * will be serialized, or otherwise it will be gracefully ignored.
 * 
 * @param <E>
 */
public final class SerializableRef<E> implements Ref<E>, Serializable {

    private static final long serialVersionUID = 1L;

    private final E _serializableObj;
    private final transient E _transientObj;

    public SerializableRef(E obj) {
        if (obj instanceof Serializable) {
            _serializableObj = obj;
            _transientObj = null;
        } else {
            _serializableObj = null;
            _transientObj = obj;
        }
    }

    @Override
    public E get() {
        if (_serializableObj == null) {
            return _transientObj;
        }
        return _serializableObj;
    }
    
    public boolean isSerializing() {
        return _serializableObj != null;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_serializableObj == null) ? 0 : _serializableObj.hashCode());
        result = prime * result + ((_transientObj == null) ? 0 : _transientObj.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SerializableRef<?> other = (SerializableRef<?>) obj;
        if (_serializableObj == null) {
            if (other._serializableObj != null)
                return false;
        } else if (!_serializableObj.equals(other._serializableObj))
            return false;
        if (_transientObj == null) {
            if (other._transientObj != null)
                return false;
        } else if (!_transientObj.equals(other._transientObj))
            return false;
        return true;
    }

}
