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

import java.util.Collection;
import java.util.Map;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.util.BooleanComparator;
import org.apache.metamodel.util.NumberComparator;

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

    protected String getString(Object value, String ifNull) {
        if (isNullOrEmpty(value)) {
            return ifNull;
        }
        return value.toString();
    }

    protected int getInt(Object value, int ifNull) {
        if (isNullOrEmpty(value)) {
            return ifNull;
        }
        return NumberComparator.toNumber(value).intValue();
    }

    protected boolean getBoolean(Object value, boolean ifNull) {
        if (isNullOrEmpty(value)) {
            return ifNull;
        }
        return BooleanComparator.toBoolean(value).booleanValue();
    }

    protected char getChar(Object value, char ifNull) {
        if (isNullOrEmpty(value)) {
            return ifNull;
        }
        if (value instanceof Character) {
            return ((Character) value).charValue();
        }
        return value.toString().charAt(0);
    }

    private static boolean isNullOrEmpty(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof String && ((String) value).isEmpty()) {
            return true;
        }
        if (value instanceof Collection && ((Collection<?>) value).isEmpty()) {
            return true;
        }
        if (value instanceof Map && ((Map<?, ?>) value).isEmpty()) {
            return true;
        }
        return false;
    }

}
