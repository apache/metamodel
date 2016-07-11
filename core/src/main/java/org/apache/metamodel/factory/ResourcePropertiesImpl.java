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
package org.apache.metamodel.factory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class ResourcePropertiesImpl implements ResourceProperties {

    private static final long serialVersionUID = 1L;

    private final Map<String, Object> map;

    public ResourcePropertiesImpl() {
        this(new HashMap<String, Object>());
    }

    public ResourcePropertiesImpl(Map<String, Object> map) {
        this.map = map;
    }

    private String getString(String key) {
        final Object value = map.get(key);
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    public URI getUri() {
        final Object uri = map.get("uri");
        if (uri == null) {
            return null;
        }
        if (uri instanceof URI) {
            return (URI) uri;
        }
        return URI.create(uri.toString());
    }

    @Override
    public Map<String, Object> toMap() {
        return map;
    }

    @Override
    public String getUsername() {
        return getString("username");
    }

    @Override
    public String getPassword() {
        return getString("password");
    }

}
