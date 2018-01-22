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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.query.SelectItem;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DataSet} implementation for ElasticSearch
 */
final class ElasticSearchRestDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestDataSet.class);

    private final ElasticSearchRestClient _client;
    private final AtomicBoolean _closed;

    private SearchResponse _searchResponse;
    private SearchHit _currentHit;
    private int _hitIndex = 0;

    public ElasticSearchRestDataSet(final ElasticSearchRestClient client, final SearchResponse searchResponse, final List<SelectItem> selectItems) {
        super(selectItems);
        _client = client;
        _searchResponse = searchResponse;
        _closed = new AtomicBoolean(false);
    }


    @Override
    public void close() {
        super.close();
        boolean closeNow = _closed.compareAndSet(true, false);
        if (closeNow) {
            final String scrollId = _searchResponse.getScrollId();
            final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            try {
                _client.execute(clearScrollRequest);
            } catch (IOException e) {
                logger.warn("Could not clear scroll.", e);            
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!_closed.get()) {
            logger.warn("finalize() invoked, but DataSet is not closed. Invoking close() on {}", this);
            close();
        }
    }

    @Override
    public boolean next() {
        final SearchHit[] hits = _searchResponse.getHits().getHits();
        if (hits.length == 0) {
            // break condition for the scroll
            _currentHit = null;
            return false;
        }

        if (_hitIndex < hits.length) {
            // pick the next hit within this search response
            _currentHit = hits[_hitIndex];
            _hitIndex++;
            return true;
        }

        final String scrollId = _searchResponse.getScrollId();
        if (scrollId == null) {
            // this search response is not scroll=able - then it's the end.
            _currentHit = null;
            return false;
        }

        
        // try to scroll to the next set of hits
        try {
            _searchResponse = _client.searchScroll(new SearchScrollRequest(scrollId).scroll(
                    ElasticSearchRestDataContext.TIMEOUT_SCROLL));
        } catch (IOException e) {
            logger.warn("Failed to scroll to the next search response set.", e);
            return false;
        }

        // start over (recursively)
        _hitIndex = 0;
        return next();
    }

    @Override
    public Row getRow() {
        if (_currentHit == null) {
            return null;
        }

        final Map<String, Object> source = _currentHit.getSource();
        final String documentId = _currentHit.getId();
        final Row row = ElasticSearchUtils.createRow(source, documentId, getHeader());
        return row;
    }
}
