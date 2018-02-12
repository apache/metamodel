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
package org.apache.metamodel.elasticsearch;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.query.SelectItem;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DataSet} implementation for ElasticSearch
 */
public abstract class AbstractElasticSearchDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory.getLogger(AbstractElasticSearchDataSet.class);

    protected final AtomicBoolean _closed;

    protected SearchResponse _searchResponse;
    protected SearchHit _currentHit;
    protected int _hitIndex = 0;

    public AbstractElasticSearchDataSet(final SearchResponse searchResponse, final List<SelectItem> selectItems) {
        super(selectItems);
        _searchResponse = searchResponse;
        _closed = new AtomicBoolean(false);
    }

    @Override
    public void close() {
        super.close();
        boolean closeNow = _closed.compareAndSet(true, false);
        if (closeNow) {
            closeNow();
        }
    }

    protected abstract void closeNow();
    
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
            // this search response is not scrolleable - then it's the end.
            _currentHit = null;
            return false;
        }

        // try to scroll to the next set of hits
        try {
            _searchResponse = scrollSearchResponse(scrollId);
        } catch (IOException e) {
            logger.warn("Failed to scroll to the next search response set.", e);
            return false;
        }

        // start over (recursively)
        _hitIndex = 0;
        return next();
    }
    
    protected abstract SearchResponse scrollSearchResponse(final String scrollId) throws IOException;

    @Override
    public Row getRow() {
        if (_currentHit == null) {
            return null;
        }

        final Map<String, Object> source = _currentHit.getSource();
        final String documentId = _currentHit.getId();
        return ElasticSearchUtils.createRow(source, documentId, getHeader());
    }
}
