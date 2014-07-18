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

import java.io.IOException;
import java.util.Map;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.Document;
import org.apache.metamodel.data.DocumentSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Utility class that provides an easy way of iterating documents in a JSON file
 */
final class JsonDocumentSource implements DocumentSource {

    private static final Logger logger = LoggerFactory.getLogger(JsonDocumentSource.class);

    private final JsonParser _parser;
    private final String _sourceCollectionName;

    public JsonDocumentSource(JsonParser parser, String sourceCollectionName) {
        _parser = parser;
        _sourceCollectionName = sourceCollectionName;
    }

    public Document next() {
        while (true) {
            final JsonToken token = getNextToken();
            if (token == null) {
                return null;
            }

            if (token == JsonToken.START_OBJECT) {
                Map<String, ?> value = readValue();
                return new Document(_sourceCollectionName, value, value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, ?> readValue() {
        try {
            return _parser.readValueAs(Map.class);
        } catch (Exception e) {
            throw new MetaModelException(e);
        }
    }

    private JsonToken getNextToken() {
        try {
            return _parser.nextToken();
        } catch (Exception e) {
            throw new MetaModelException(e);
        }
    }

    @Override
    public void close() {
        try {
            _parser.close();
        } catch (IOException e) {
            logger.warn("Failed to ");
        }
    }
}
