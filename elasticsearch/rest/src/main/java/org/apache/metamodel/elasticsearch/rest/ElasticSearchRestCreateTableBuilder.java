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
package org.apache.metamodel.elasticsearch.rest;

import java.util.Map;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.create.AbstractTableCreationBuilder;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;

final class ElasticSearchRestCreateTableBuilder extends AbstractTableCreationBuilder<ElasticSearchRestUpdateCallback> {

    public ElasticSearchRestCreateTableBuilder(final ElasticSearchRestUpdateCallback updateCallback,
            final Schema schema, final String name) {
        super(updateCallback, schema, name);
    }

    @Override
    public Table execute() throws MetaModelException {
        final MutableTable table = getTable();
        final Map<String, ?> source = ElasticSearchUtils.getMappingSource(table);

        final ElasticSearchRestDataContext dataContext = getUpdateCallback().getDataContext();
        final String indexName = dataContext.getIndexName();

        final PutMappingRequest putMapping = new PutMappingRequest(indexName).type(table.getName()).source(source);
        getUpdateCallback().execute(putMapping);

        final MutableSchema schema = (MutableSchema) getSchema();
        schema.addTable(table);
        return table;
    }

}
