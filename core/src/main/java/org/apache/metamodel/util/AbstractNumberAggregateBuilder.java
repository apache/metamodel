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

/**
 * Abstract {@link AggregateBuilder} that works on numbers.
 * 
 * This will skip nulls and empty strings, but otherwise try to convert to
 * numbers.
 *
 * @param <N>
 */
public abstract class AbstractNumberAggregateBuilder<N extends Number> implements AggregateBuilder<N> {

    @Override
    public final void add(Object o) {
        if (o == null) {
            return;
        }
        if (o instanceof String) {
            if ("".equals(((String) o).trim())) {
                return;
            }
        }

        final Number number = NumberComparator.toNumber(o);
        if (number == null) {
            throw new IllegalArgumentException("Could not convert to number: " + o);
        }
        add(number);
    }

    /**
     * Adds a non-null number to the aggregate calculation.
     * 
     * @param number
     */
    protected abstract void add(Number number);

}
