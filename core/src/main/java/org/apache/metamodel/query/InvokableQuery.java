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

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;

/**
 * Represents a {@link Query} or query-builder object that's aware of it's {@link DataContext} and is ready to execute
 * or compile.
 */
public interface InvokableQuery {

    /**
     * Compiles the query
     * 
     * @return the {@link CompiledQuery} that is is returned by compiling the query.
     */
    public CompiledQuery compile();

    /**
     * Executes the query.
     * 
     * @return the {@link DataSet} that is returned by executing the query.
     */
    public DataSet execute();
}
