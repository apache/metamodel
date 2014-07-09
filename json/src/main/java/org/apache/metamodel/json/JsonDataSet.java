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
package org.apache.metamodel.json;

import java.util.Map;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.builder.DocumentConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JsonDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory.getLogger(JsonDataSet.class);

    private final JsonDocumentSource _jsonDocumentSource;
    private final DocumentConverter _converter;
    private volatile Map<String, ?> _map;

    public JsonDataSet(DataSetHeader header, JsonDocumentSource jsonDocumentSource, DocumentConverter converter) {
        super(header);
        _jsonDocumentSource = jsonDocumentSource;
        _converter = converter;
    }

    @Override
    public boolean next() {
        _map = _jsonDocumentSource.next();
        if (_map == null) {
            return false;
        }
        return true;
    }

    @Override
    public Row getRow() {
        if (_map == null) {
            return null;
        }
        final DataSetHeader header = getHeader();
        return _converter.convert(_map, header);
    }

    @Override
    public void close() {
        super.close();
        try {
            _jsonDocumentSource.close();
        } catch (Exception e) {
            logger.warn("Failed to close JSON parser", e);
        }
    }
}
