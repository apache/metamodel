/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.elasticsearch.AbstractElasticSearchDataContext;
import org.apache.metamodel.elasticsearch.AbstractElasticSearchDataSet;
import org.apache.metamodel.query.SelectItem;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DataSet} implementation for ElasticSearch
 */
final class ElasticSearchRestDataSet extends AbstractElasticSearchDataSet {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestDataSet.class);

    private final ElasticSearchRestClient _client;

    public ElasticSearchRestDataSet(final ElasticSearchRestClient client, final SearchResponse searchResponse, final List<SelectItem> selectItems) {
        super(searchResponse, selectItems);
        _client = client;
    }

    @Override
    public void closeNow() {
        final String scrollId = _searchResponse.getScrollId();
        final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        try {
            _client.execute(clearScrollRequest);
        } catch (IOException e) {
            logger.warn("Could not clear scroll.", e);
        }
    }

    @Override
    protected SearchResponse scrollSearchResponse(final String scrollId) throws IOException {
        return _client.searchScroll(new SearchScrollRequest(scrollId).scroll(
                AbstractElasticSearchDataContext.TIMEOUT_SCROLL));
    }
}
