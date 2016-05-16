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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.SearchScroll;
import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link DataSet} implementation for ElasticSearch
 */
final class JestElasticSearchDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory.getLogger(JestElasticSearchDataSet.class);

    private final JestClient _client;
    private final AtomicBoolean _closed;

    private JestResult _searchResponse;
    private JsonObject _currentHit;
    private int _hitIndex = 0;

    public JestElasticSearchDataSet(JestClient client, JestResult searchResponse, List<SelectItem> selectItems) {
        super(selectItems);
        _client = client;
        _searchResponse = searchResponse;
        _closed = new AtomicBoolean(false);
    }

    public JestElasticSearchDataSet(JestClient client, JestResult searchResponse, Column[] columns) {
        super(columns);
        _client = client;
        _searchResponse = searchResponse;
        _closed = new AtomicBoolean(false);
    }

    @Override
    public void close() {
        super.close();
        boolean closeNow = _closed.compareAndSet(true, false);
        if (closeNow) {
            final String scrollId = _searchResponse.getJsonObject().getAsJsonPrimitive("_scroll_id").getAsString();
            JestClientExecutor.execute(_client, new JestDeleteScroll.Builder(scrollId).build(), false);
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
        final JsonArray hits = _searchResponse.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits");
        if (hits.size() == 0) {
            // break condition for the scroll
            _currentHit = null;
            return false;
        }

        if (_hitIndex < hits.size()) {
            // pick the next hit within this search response
            _currentHit = hits.get(_hitIndex).getAsJsonObject();
            _hitIndex++;
            return true;
        }

        final JsonPrimitive scrollId = _searchResponse.getJsonObject().getAsJsonPrimitive("_scroll_id");
        if (scrollId == null) {
            // this search response is not scrollable - then it's the end.
            _currentHit = null;
            return false;
        }

        // try to scroll to the next set of hits
        final SearchScroll scroll = new SearchScroll.Builder(scrollId.getAsString(), ElasticSearchRestDataContext.TIMEOUT_SCROLL).build();

        _searchResponse = JestClientExecutor.execute(_client, scroll);

        // start over (recursively)
        _hitIndex = 0;
        return next();
    }

    @Override
    public Row getRow() {
        if (_currentHit == null) {
            return null;
        }

        final JsonObject source = _currentHit.getAsJsonObject("_source");
        final String documentId = _currentHit.get("_id").getAsString();
        return JestElasticSearchUtils.createRow(source, documentId, getHeader());

    }
}
