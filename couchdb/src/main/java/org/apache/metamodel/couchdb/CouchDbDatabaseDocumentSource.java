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
package org.apache.metamodel.couchdb;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.metamodel.data.Document;
import org.apache.metamodel.data.DocumentSource;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.StreamingViewResult;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult.Row;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * {@link DocumentSource} implementation that goes with the
 * {@link CouchDbDataContext}
 */
final class CouchDbDatabaseDocumentSource implements DocumentSource {

    private final CouchDbInstance _couchDbInstance;
    private final StreamingViewResult _view;
    private final String _databaseName;
    private final Iterator<Row> _rowIterator;
    private final AtomicBoolean _closed;

    public CouchDbDatabaseDocumentSource(CouchDbInstance couchDbInstance, String databaseName, int maxRows) {
        _couchDbInstance = couchDbInstance;
        _databaseName = databaseName;
        _closed = new AtomicBoolean(false);

        final CouchDbConnector tableConnector = _couchDbInstance.createConnector(databaseName, false);
        if (maxRows > -1) {
            _view = tableConnector.queryForStreamingView(new ViewQuery().allDocs().includeDocs(true).limit(maxRows));
        } else {
            _view = tableConnector.queryForStreamingView(new ViewQuery().allDocs().includeDocs(true));
        }
        _rowIterator = _view.iterator();
    }

    @Override
    public Document next() {
        if (_closed.get()) {
            return null;
        }

        if (!CouchDbUtils.safeHasNext(_rowIterator)) {
            close();
            return next();
        }

        final Row row = _rowIterator.next();
        final JsonNode docNode = row.getDocAsNode();
        final Map<String, ?> map = CouchDbUtils.jsonNodeToMap(docNode);
        return new Document(_databaseName, map, row);
    }

    @Override
    public void close() {
        boolean closedBefore = _closed.getAndSet(true);
        if (!closedBefore) {
            _view.close();
        }
    }

}
