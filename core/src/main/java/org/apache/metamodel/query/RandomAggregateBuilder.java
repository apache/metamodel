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
package org.apache.metamodel.query;

import java.util.Random;

import org.apache.metamodel.util.AggregateBuilder;

public class RandomAggregateBuilder implements AggregateBuilder<Object> {

    private Object _result;
    private long _count;
    private final Random _random;

    public RandomAggregateBuilder() {
        _result = null;
        _count = 0;
        _random = new Random();
    }

    @Override
    public void add(Object o) {
        if (o == null) {
            return;
        }

        _count++;

        if (_random.nextDouble() < (1d / _count)) {
            _result = o;
        }
    }

    @Override
    public Object getAggregate() {
        return _result;
    }

}
