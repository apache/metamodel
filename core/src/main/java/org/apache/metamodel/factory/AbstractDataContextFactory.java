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

import org.apache.metamodel.DataContext;

/**
 * Abstract implementation of {@link DataContextFactory} with utility methods for easier subclass implementation.
 */
public abstract class AbstractDataContextFactory implements DataContextFactory {

    /**
     * Gets the "type" value for the {@link DataContext}s that this factory produces. By convention these are lower-case
     * terms such as "csv" or "jdbc", and separated by dash in case it requires multiple terms, e.g. "fixed-width".
     * 
     * @return
     */
    protected abstract String getType();

    @Override
    public final boolean accepts(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry) {
        return getType().equals(properties.getDataContextType());
    }

    protected String getString(String value, String ifNull) {
        return value == null ? ifNull : value;
    }

    protected int getInt(Integer value, int ifNull) {
        return value == null ? ifNull : value;
    }

    protected boolean getBoolean(Boolean value, boolean ifNull) {
        return value == null ? ifNull : value;
    }

    protected char getChar(Character value, char ifNull) {
        return value == null ? ifNull : value;
    }
}
