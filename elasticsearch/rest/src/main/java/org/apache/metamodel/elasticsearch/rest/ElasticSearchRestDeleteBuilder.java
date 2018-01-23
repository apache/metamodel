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
import java.util.Iterator;
import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.delete.AbstractRowDeletionBuilder;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.LogicalOperator;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * {@link RowDeletionBuilder} implementation for
 * {@link ElasticSearchRestDataContext}.
 */
final class ElasticSearchRestDeleteBuilder extends AbstractRowDeletionBuilder {
    private final ElasticSearchRestUpdateCallback _updateCallback;

    public ElasticSearchRestDeleteBuilder(final ElasticSearchRestUpdateCallback updateCallback, final Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        final Table table = getTable();
        final String documentType = table.getName();

        final ElasticSearchRestDataContext dataContext = _updateCallback.getDataContext();
        final String indexName = dataContext.getIndexName();

        final List<FilterItem> whereItems = getWhereItems();

        // delete by query - note that creteQueryBuilderForSimpleWhere may
        // return matchAllQuery() if no where items are present.
        final QueryBuilder queryBuilder = ElasticSearchUtils.createQueryBuilderForSimpleWhere(whereItems,
                LogicalOperator.AND);
        if (queryBuilder == null) {
            // TODO: The where items could not be pushed down to a query. We
            // could solve this by running a query first, gather all
            // document IDs and then delete by IDs.
            throw new UnsupportedOperationException("Could not push down WHERE items to delete by query request: "
                    + whereItems);
        }
        
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(documentType);
        searchRequest.source(searchSourceBuilder);

        try {
            final SearchResponse response = dataContext.getElasticSearchClient().search(searchRequest);

            final Iterator<SearchHit> iterator = response.getHits().iterator();
            while (iterator.hasNext()) {
                final SearchHit hit = iterator.next();
                final String typeId = hit.getId();

                DeleteRequest deleteRequest = new DeleteRequest(indexName, documentType, typeId);

                _updateCallback.execute(deleteRequest);

            }
        } catch (IOException e) {
            throw new MetaModelException(e);
        }
    }
}
