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

import org.apache.metamodel.MetaModelException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.*;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;

/**
 * {@link DataSet} implementation for Solr.
 */
final class SolrDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory
            .getLogger(SolrDataSet.class);

    private final AtomicBoolean _closed;
    private final SolrDocumentList _docs;
    private final Map<String, List<FacetField.Count>> _facetMap;

    private Column[] _columns;
    private int _numColumns = 0;
    private int _hitIndex = 0; //current pointer in the document list
    private int _docListSize = 0;
    private int _facetFieldListSize = 0;

    private HttpSolrServer _server; //server object to use for batch fetches
    private SolrQuery _solrQuery;   //SolrJ Query object
    private String _cursorMark = ""; //cursor to determine offset for next fetch
    private QueryResponse _rsp;     //SolrJ Query Response object
    private SolrDataSet _dataSet;   //MM DataSet object to populate
    private int _limit;

    final private static int BATCH_SIZE = 500; //fetch results greater than the limit in batches of this size

    /**
     * Constructor to create SolrDataSet.
     * @param docs object encapsulating Solr Documents
     * @param columns array of MM Column objects
    **/
    public SolrDataSet(final SolrDocumentList docs, final Column[] columns) {
        super(columns);
        _numColumns = columns.length;
        _columns = columns;

        _docs = docs;
        _facetMap = null;
        _docListSize = _docs.size();

        _closed = new AtomicBoolean(false);
    }

    /**
     * Constructor to create SolrDataSet.
     * @param server Solrserver object for use in pagination
     * @param queryStr search string
     * @param columns array of MM Column objects
     * @param limit result limit
    **/
    public SolrDataSet(final HttpSolrServer server, final String queryStr, final Column[] columns, final int limit) {
        super(columns);
        _numColumns = columns.length;
        _columns = columns;

        _docs = null;
        _facetMap = null;
        _docListSize = 0;

        _server    = server;
        _solrQuery = new SolrQuery(queryStr).setRows(BATCH_SIZE).setSort(SortClause.asc("id"));
        _limit     = limit;

        _closed = new AtomicBoolean(false);
    }

    /**
     * Constructor to create SolrDataSet
     * @param facetFieldsList List of facet fields
     * @param columns array of MM Column objects
    **/
    public SolrDataSet(final List<FacetField> facetFieldsList, final Column[] columns) {
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
        if (_server != null) {
            if (_dataSet != null && _dataSet.next()) {
                return true;
            }

            if (_cursorMark.isEmpty()) {
                _cursorMark = CursorMarkParams.CURSOR_MARK_START;
                return true;
            } else {
                String nextCursorMark = _rsp.getNextCursorMark();
                if (_cursorMark.equals(nextCursorMark) || _hitIndex >= _limit) {
                    return false;
                } else {
                    _cursorMark = nextCursorMark;
                    return true;
                }
            }
        }

        if (_docListSize != 0 && _docListSize == _hitIndex) {
            return false;
        }

        if (_facetFieldListSize != 0 && _facetFieldListSize == _hitIndex) {
            return false;
        }

        if (_docListSize == 0 && _facetFieldListSize == 0) {
            return false;
        }

        return true;
    }

    /**
     * Fetch rows of data based on the cursor offset.
     * If data is not available in the DataSet object, then do another fetch
     * @return Row a result row
    **/
    private Row getBatchRow() {
        if (_dataSet != null && _dataSet.next()) {
            ++_hitIndex;
            return _dataSet.getRow();
        }

        _solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, _cursorMark);

        try {
            _rsp = _server.query(_solrQuery);
            _dataSet = new SolrDataSet(_rsp.getResults(), _columns);
        } catch (Exception e) {
            logger.error("Search query for documents failed", "", e);
            throw new MetaModelException("Query failed " + e);
        }

        if (_dataSet != null && _dataSet.next()) {
            ++_hitIndex;
            return _dataSet.getRow();
        }

        return null;
    }

    /**
     * Fetch a row of data (Document values) based on the current offset.
     * @param docs SolrDocumentList object containing the documents
     * @return Object[] object array of (document) values
    **/
    private Object[] getRow(final SolrDocumentList docs) {
        Object[] values = new Object[_numColumns];
        SolrDocument doc = docs.get(_hitIndex);

        Map<String, Object> docValues = doc.getFieldValueMap();

        for (int i = 0; i < _columns.length; i++) {
            String key = _columns[i].getName();
            values[i] = docValues.get(key);
        }

        _hitIndex++;

        return values;
    }

    /**
     * Fetch a row of data (Facet values) based on the current offset.
     * @param facetMap SolrDocumentList object containing the documents
     * @return Object[] object array of (document) values
    **/
    private Object[] getRow(final Map<String, List<FacetField.Count>> facetMap) {
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

        if (_server != null) {
            return getBatchRow();
        } else {
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
}

