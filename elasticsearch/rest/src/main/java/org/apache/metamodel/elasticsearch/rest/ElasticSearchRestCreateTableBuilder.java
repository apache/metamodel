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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.create.AbstractTableCreationBuilder;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ElasticSearchRestCreateTableBuilder extends AbstractTableCreationBuilder<ElasticSearchRestUpdateCallback> {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestCreateTableBuilder.class);

    public ElasticSearchRestCreateTableBuilder(final ElasticSearchRestUpdateCallback updateCallback,
            final Schema schema, final String name) {
        super(updateCallback, schema, name);
    }

    @Override
    public Table execute() {
        final MutableTable table = getTable();
        final Map<String, ?> source = ElasticSearchUtils.getMappingSource(table);

        final ElasticSearchRestDataContext dataContext = getUpdateCallback().getDataContext();
        final String indexName = dataContext.getIndexName();

        final List<Table> tables = dataContext.getDefaultSchema().getTables();
        
        if (!(tables.isEmpty() || tables.get(0).getName().equals(table.getName()))) {
            throw new MetaModelException("Can't add more than one table to Elasticsearch index.");
        }
        
        try {
            dataContext
                    .getRestHighLevelClient()
                    .indices()
                    .putMapping(new PutMappingRequest(indexName).source(source), RequestOptions.DEFAULT);
        } catch (final IOException e) {
            logger.warn("Could not update mappings.", e);
            throw new MetaModelException("Could not update mappings.", e);
        }

        final MutableSchema schema = (MutableSchema) getSchema();
        schema.addTable(table);
        return table;
    }

}
