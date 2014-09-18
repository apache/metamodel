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
package org.apache.metamodel.elasticsearch;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

final class ElasticSearchDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory
            .getLogger(ElasticSearchDataSet.class);

    private int readCount = 0;

    private final SearchHit[] _cursor;
    private final boolean _queryPostProcessed;

    private boolean _closed;
    private volatile SearchHit _dbObject;

    public ElasticSearchDataSet(SearchResponse cursor, Column[] columns,
                          boolean queryPostProcessed) {
        super(columns);
        _cursor = cursor.getHits().hits();
        _queryPostProcessed = queryPostProcessed;
        _closed = false;
    }

    public boolean isQueryPostProcessed() {
        return _queryPostProcessed;
    }

    @Override
    public void close() {
        super.close();
        //_cursor.close();
        _closed = true;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!_closed) {
            logger.warn(
                    "finalize() invoked, but DataSet is not closed. Invoking close() on {}",
                    this);
            close();
        }
    }

    @Override
    public boolean next() {
        if (readCount<_cursor.length) {
            _dbObject = _cursor[readCount];
            readCount++;
            return true;
        } else {
            _dbObject = null;
            return false;
        }
    }

    @Override
    public Row getRow() {
        if (_dbObject == null) {
            return null;
        }

        final int size = getHeader().size();
        final Object[] values = new Object[size];
        for (int i = 0; i < values.length; i++) {
            final SelectItem selectItem = getHeader().getSelectItem(i);
            final Map<String, Object> element = _dbObject.getSource();
            final String key = selectItem.getColumn().getName();
            values[i] = element.get(key);
        }
        return new DefaultRow(getHeader(), values);
    }
}
