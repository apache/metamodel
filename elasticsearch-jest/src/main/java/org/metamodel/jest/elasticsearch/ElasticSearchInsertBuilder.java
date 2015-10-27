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
package org.metamodel.jest.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.params.Parameters;

final class ElasticSearchInsertBuilder extends AbstractRowInsertionBuilder<ElasticSearchUpdateCallback> {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchInsertBuilder.class);

    public ElasticSearchInsertBuilder(ElasticSearchUpdateCallback updateCallback, Table table) {
        super(updateCallback, table);
    }

    @Override
    public void execute() throws MetaModelException {
        final ElasticSearchDataContext dataContext = getUpdateCallback().getDataContext();
        final String indexName = dataContext.getIndexName();
        final String documentType = getTable().getName();


        final Map<String, Object> source = new HashMap<>();
        final Column[] columns = getColumns();
        final Object[] values = getValues();
        String id = null;
        for (int i = 0; i < columns.length; i++) {
            if (isSet(columns[i])) {
                final String name = columns[i].getName();
                final Object value = values[i];
                if (ElasticSearchDataContext.FIELD_ID.equals(name)) {
                    if (value != null) {
                        id = value.toString();
                    }
                } else {
                    source.put(name, value);
                }
            }
        }

        assert !source.isEmpty();

        Index index = new Index.Builder(source).index(indexName).type(documentType).id(id).setParameter(Parameters.OP_TYPE, "create").build();

        final DocumentResult result;
        try {
            result = dataContext.getElasticSearchClient().execute(index);
        } catch (IOException e) {
            logger.warn("Could not index document", e);
            throw new MetaModelException("Could not index document", e);
        }

        logger.debug("Inserted document: id={}", result.getId());
    }

}
