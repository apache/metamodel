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
package org.apache.metamodel.util;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * A serializable reference to an object which may or may not be serializable.
 * Using this reference there is safety that if the object IS serializable, it
 * will be serialized, or otherwise it will be gracefully ignored.
 * 
 * @param <E>
 */
public final class SerializableRef<E> implements Supplier<E>, Serializable {

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
