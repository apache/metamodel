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
package org.apache.metamodel.elasticsearch.nativeclient;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.LogicalOperator;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.AbstractRowUpdationBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchUpdateBuilder extends AbstractRowUpdationBuilder {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchUpdateBuilder.class);

    private final ElasticSearchUpdateCallback _updateCallback;

    public ElasticSearchUpdateBuilder(ElasticSearchUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {

        final Table table = getTable();
        final String documentType = table.getName();

        final ElasticSearchDataContext dataContext = _updateCallback.getDataContext();
        final Client client = dataContext.getElasticSearchClient();
        final String indexName = dataContext.getIndexName();
        final List<FilterItem> whereItems = getWhereItems();

        // delete by query - note that creteQueryBuilderForSimpleWhere may
        // return matchAllQuery() if no where items are present.
        final QueryBuilder queryBuilder =
                ElasticSearchUtils.createQueryBuilderForSimpleWhere(whereItems, LogicalOperator.AND);
        if (queryBuilder == null) {
            // TODO: The where items could not be pushed down to a query. We
            // could solve this by running a query first, gather all
            // document IDs and then delete by IDs.
            throw new UnsupportedOperationException(
                    "Could not push down WHERE items to delete by query request: " + whereItems);
        }

        final SearchResponse response = client.prepareSearch(indexName).setQuery(queryBuilder).execute().actionGet();

        final Iterator<SearchHit> iterator = response.getHits().iterator();
        while (iterator.hasNext()) {
            final SearchHit hit = iterator.next();
            final String typeId = hit.getId();

            final UpdateRequestBuilder requestBuilder =
                    new UpdateRequestBuilder(client, UpdateAction.INSTANCE).setIndex(indexName).setType(documentType)
                            .setId(typeId);

            final Map<String, Object> valueMap = new HashMap<>();
            final Column[] columns = getColumns();
            final Object[] values = getValues();
            for (int i = 0; i < columns.length; i++) {
                if (isSet(columns[i])) {
                    final String name = columns[i].getName();
                    final Object value = values[i];
                    if (ElasticSearchUtils.FIELD_ID.equals(name)) {
                        if (value != null) {
                            requestBuilder.setId(value.toString());
                        }
                    } else {
                        valueMap.put(name, value);
                    }
                }
            }

            assert !valueMap.isEmpty();

            requestBuilder.setDoc(valueMap);

            final UpdateResponse updateResponse = requestBuilder.execute().actionGet();

            logger.debug("Update document: id={}", updateResponse.getId());

            client.admin().indices().prepareRefresh(indexName).get();
        }
    }
}
