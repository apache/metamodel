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

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.ektorp.StreamingViewResult;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * DataSet implementation for couch db.
 */
final class CouchDbDataSet extends AbstractDataSet {

    private final Iterator<org.ektorp.ViewResult.Row> _iterator;
    private final StreamingViewResult _streamingViewResult;
    private Row _row;

    public CouchDbDataSet(List<SelectItem> selectItems, StreamingViewResult streamingViewResult) {
        super(selectItems);
        _streamingViewResult = streamingViewResult;

        _iterator = _streamingViewResult.iterator();
    }

    @Override
    public boolean next() {
        if (_iterator == null || !_iterator.hasNext()) {
            return false;
        }

        final org.ektorp.ViewResult.Row row = _iterator.next();
        final JsonNode node = row.getDocAsNode();

        final DataSetHeader header = getHeader();
        _row = CouchDbUtils.jsonNodeToMetaModelRow(node, header);

        return true;
    }

    @Override
    public Row getRow() {
        return _row;
    }

    @Override
    public void close() {
        super.close();
        _streamingViewResult.close();
    }

}
