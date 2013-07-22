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
package org.apache.metamodel.intercept;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.schema.Table;

final class InterceptableTableDropBuilder implements TableDropBuilder {

    private final TableDropBuilder _tableDropBuilder;
    private final InterceptorList<TableDropBuilder> _tableDropInterceptors;

    public InterceptableTableDropBuilder(TableDropBuilder tableDropBuilder,
            InterceptorList<TableDropBuilder> tableDropInterceptors) {
        _tableDropBuilder = tableDropBuilder;
        _tableDropInterceptors = tableDropInterceptors;
    }

    @Override
    public Table getTable() {
        return _tableDropBuilder.getTable();
    }

    @Override
    public String toSql() {
        return _tableDropBuilder.toSql();
    }

    @Override
    public void execute() throws MetaModelException {
        TableDropBuilder tableDropBuilder = _tableDropBuilder;
        tableDropBuilder = _tableDropInterceptors.interceptAll(_tableDropBuilder);
        tableDropBuilder.execute();
    }

}
