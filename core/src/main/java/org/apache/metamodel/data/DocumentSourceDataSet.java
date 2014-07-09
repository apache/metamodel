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
package org.apache.metamodel.data;

import org.apache.metamodel.convert.DocumentConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DataSet} that uses a {@link DocumentSource} as it's source.
 */
public class DocumentSourceDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory.getLogger(DocumentSourceDataSet.class);

    private final DocumentSource _documentSource;
    private final DocumentConverter _converter;
    private volatile Document _document;

    public DocumentSourceDataSet(DataSetHeader header, DocumentSource documentSource, DocumentConverter converter) {
        super(header);
        _documentSource = documentSource;
        _converter = converter;
    }

    @Override
    public boolean next() {
        _document = _documentSource.next();
        if (_document == null) {
            return false;
        }
        return true;
    }

    @Override
    public Row getRow() {
        if (_document == null) {
            return null;
        }
        final DataSetHeader header = getHeader();
        return _converter.convert(_document, header);
    }

    @Override
    public void close() {
        super.close();
        try {
            _documentSource.close();
        } catch (Exception e) {
            logger.warn("Failed to close DocumentSource: {}", _document, e);
        }
    }
}
