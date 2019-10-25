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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ElasticSearchRestInsertBuilder extends AbstractRowInsertionBuilder<ElasticSearchRestUpdateCallback> {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestInsertBuilder.class);

    public ElasticSearchRestInsertBuilder(final ElasticSearchRestUpdateCallback updateCallback, final Table table) {
        super(updateCallback, table);
    }

    @Override
    public void execute() throws MetaModelException {
        final ElasticSearchRestUpdateCallback updateCallback = getUpdateCallback();
        final ElasticSearchRestDataContext dataContext = updateCallback.getDataContext();
        final String indexName = dataContext.getIndexName();
        final List<Table> tables = dataContext.getDefaultSchema().getTables();
        
        if (!(tables.isEmpty() || tables.get(0).getName().equals(getTable().getName()))) {
            throw new MetaModelException("Can't add more than one table to Elasticsearch index.");
        } 
        
        final Map<String, Object> source = new HashMap<>();
        final Column[] columns = getColumns();
        final Object[] values = getValues();
        String id = null;
        for (int i = 0; i < columns.length; i++) {
            if (isSet(columns[i])) {
                final String columnName = columns[i].getName();

                final Object value = values[i];
                if (ElasticSearchUtils.FIELD_ID.equals(columnName)) {
                    if (value != null) {
                        id = value.toString();
                    }
                } else {
                    final String fieldName = ElasticSearchUtils.getValidatedFieldName(columnName);
                    source.put(fieldName, value);
                }
            }
        }

        if (!source.isEmpty()) {
            IndexRequest indexRequest = new IndexRequest(indexName).id(id);
            indexRequest.source(source);

            getUpdateCallback().execute(indexRequest);
        } else {
            logger.info("Source is empty, no index request is executed.");
        }
    }

}
