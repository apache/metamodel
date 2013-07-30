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
package org.apache.metamodel.hbase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public class HBaseFamilyMap implements Map<Object, Object> {

    private final NavigableMap<byte[], byte[]> _map;

    public HBaseFamilyMap(NavigableMap<byte[], byte[]> map) {
        _map = map;
    }

    @Override
    public int size() {
        return _map.size();
    }

    @Override
    public boolean isEmpty() {
        return _map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return _map.containsKey(ByteUtils.toBytes(key));
    }

    @Override
    public boolean containsValue(Object value) {
        return _map.containsValue(ByteUtils.toBytes(value));
    }

    @Override
    public Object get(Object key) {
        return _map.get(ByteUtils.toBytes(key));
    }

    @Override
    public Object put(Object key, Object value) {
        throw new UnsupportedOperationException("HBase row value map is immutable");
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException("HBase row value map is immutable");
    }

    @Override
    public void putAll(Map<? extends Object, ? extends Object> m) {
        throw new UnsupportedOperationException("HBase row value map is immutable");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("HBase row value map is immutable");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Object> keySet() {
        Set<?> keySet = _map.keySet();
        return (Set<Object>) keySet;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<Object> values() {
        Collection<?> values = _map.values();
        return (Collection<Object>) values;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<java.util.Map.Entry<Object, Object>> entrySet() {
        final Set<?> entrySet = _map.entrySet();
        return (Set<java.util.Map.Entry<Object, Object>>) entrySet;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (java.util.Map.Entry<byte[], byte[]> entry : _map.entrySet()) {
            if (sb.length() > 1) {
                sb.append(',');
            }
            sb.append(Arrays.toString(entry.getKey()));
            sb.append('=');
            sb.append(Arrays.toString(entry.getValue()));
        }
        sb.append('}');
        return sb.toString();
    }
}
