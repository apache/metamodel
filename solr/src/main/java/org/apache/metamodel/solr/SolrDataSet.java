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
package org.apache.metamodel.solr;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.schema.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

/**
 * {@link DataSet} implementation for Solr 
 */
final class SolrDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory
            .getLogger(SolrDataSet.class);

    private final AtomicBoolean _closed;
    private final SolrDocumentList _docs;
    private final Map<String, List<FacetField.Count>> _facetMap;

    private Column[] _columns;
    private int _numColumns = 0;
    private int _hitIndex = 0;
    private int _docListSize = 0;
    private int _facetFieldListSize = 0;

    public SolrDataSet(SolrDocumentList _docs, Column[] columns) {
        super(columns);
        _numColumns = columns.length;
        _columns = columns;
        this._docs = _docs;

        _facetMap = null;
        _docListSize = _docs.size();
        _closed = new AtomicBoolean(false);
    }

    public SolrDataSet(List<FacetField> facetFieldsList, Column[] columns) {
        super(columns);
        _numColumns = columns.length;
        _columns = columns;

        Map<String, List<FacetField.Count>> facetMap = new IdentityHashMap<String, List<FacetField.Count>>();

        for (FacetField facetField : facetFieldsList) {
            String facetName = facetField.getName();
            List<FacetField.Count> facetPairs = facetField.getValues();
            facetMap.put(facetName, facetPairs);
            _facetFieldListSize = facetPairs.size();
        }

        _facetMap = facetMap;
        _docs = null;
        _closed = new AtomicBoolean(false);
    }

    @Override
    public void close() {
        super.close();
        boolean closeNow = _closed.compareAndSet(true, false);
        if (closeNow) {
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!_closed.get()) {
            logger.warn(
                    "finalize() invoked, but DataSet is not closed. Invoking close() on {}",
                    this);
            close();
        }
    }

    @Override
    public boolean next() {
        if (_docListSize != 0 && _docListSize == _hitIndex)
            return false;

        if (_facetFieldListSize != 0 && _facetFieldListSize == _hitIndex)
            return false;

        if (_docListSize == 0 && _facetFieldListSize == 0)
            return false;

        return true;
    }

    private Object[] getRow(SolrDocumentList _docs) {
        Object[] values = new Object[_numColumns];
        SolrDocument doc = _docs.get(_hitIndex);

        Map<String, Object> docValues = doc.getFieldValueMap();

        for (int i = 0; i < _columns.length; i++) {
            String key = _columns[i].getName();
            values[i] = docValues.get(key);
        }

        _hitIndex++;

        return values;
    }

    private Object[] getRow(Map<String, List<FacetField.Count>> facetMap) {
        Set<String> keys = facetMap.keySet();
        Object[] values = new Object[2];

        for (String key : keys) {
            List<FacetField.Count> facetPairs = facetMap.get(key);
            FacetField.Count facetPair = facetPairs.get(_hitIndex);

            long facetCount = facetPair.getCount();
            String nameValue = facetPair.getName();

            values[0] = (Object) facetCount;
            values[1] = (Object) nameValue;
        }

        _hitIndex++;

        return values;
    }

    @Override
    public Row getRow() {
        DataSetHeader dataSetHeader = super.getHeader();

        Object[] values;

        if (_docListSize > 0) {
            values = getRow(_docs);
        } else {
            values = getRow(_facetMap);
        }

        final Row row = new DefaultRow(dataSetHeader, values);
        return row;
    }
}
