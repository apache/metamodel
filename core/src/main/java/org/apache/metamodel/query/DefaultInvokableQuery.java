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

import java.util.Objects;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;

/**
 * Default implementation of {@link InvokableQuery}, based on a ready to go {@link Query} and it's {@link DataContext}.
 */
final class DefaultInvokableQuery implements InvokableQuery {

    private final Query _query;
    private final DataContext _dataContext;

    public DefaultInvokableQuery(Query query, DataContext dataContext) {
        _query = Objects.requireNonNull(query);
        _dataContext = Objects.requireNonNull(dataContext);
    }

    @Override
    public CompiledQuery compile() {
        return _dataContext.compileQuery(_query);
    }

    @Override
    public DataSet execute() {
        return _dataContext.executeQuery(_query);
    }

    @Override
    public String toString() {
        return "DefaultInvokableQuery[" + _query + "]";
    }
}
