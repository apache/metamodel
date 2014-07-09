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
import java.util.List;

import org.apache.metamodel.data.Document;
import org.apache.metamodel.data.DocumentSource;
import org.ektorp.CouchDbInstance;

/**
 * A {@link DocumentSource} that instantiates several other
 * {@link CouchDbDatabaseDocumentSource} on-demand when they are need to provide
 * a cross-database sample
 */
public class CouchDbSamplingDocumentSource implements DocumentSource {

    private final CouchDbInstance _couchDbInstance;
    private final Iterator<String> _databaseNameIterator;
    private volatile CouchDbDatabaseDocumentSource _currentSource;

    public CouchDbSamplingDocumentSource(CouchDbInstance couchDbInstance) {
        _couchDbInstance = couchDbInstance;
        final List<String> allDatabases = _couchDbInstance.getAllDatabases();
        _databaseNameIterator = allDatabases.iterator();
        _currentSource = null;
    }

    @Override
    public Document next() {
        if (_currentSource == null) {
            if (!_databaseNameIterator.hasNext()) {
                return null;
            }
            String databaseName = _databaseNameIterator.next();
            while (databaseName.startsWith("_")) {
                if (!_databaseNameIterator.hasNext()) {
                    return null;
                }
                databaseName = _databaseNameIterator.next();
            }
            _currentSource = new CouchDbDatabaseDocumentSource(_couchDbInstance, databaseName, 500);
        }

        final Document next = _currentSource.next();
        if (next == null) {
            if (_currentSource != null) {
                _currentSource.close();
            }
            _currentSource = null;
            return next();
        }

        return next;
    }

    @Override
    public void close() {
        if (_currentSource != null) {
            _currentSource.close();
        }
    }

}
