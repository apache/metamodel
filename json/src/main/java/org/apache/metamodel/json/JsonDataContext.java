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

import java.io.File;
import java.io.InputStream;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.CachingDataSetHeader;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.MaxRowsDataSet;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.builder.DocumentConverter;
import org.apache.metamodel.schema.builder.DocumentSource;
import org.apache.metamodel.schema.builder.LazyDocumentSource;
import org.apache.metamodel.schema.builder.SchemaBuilder;
import org.apache.metamodel.schema.builder.SingleTableInferentialSchemaBuilder;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.LazyRef;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.ResourceUtils;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DataContext} implementation that works on JSON files or
 * {@link Resource}s.
 */
public class JsonDataContext extends QueryPostprocessDataContext {

    private static final Logger logger = LoggerFactory.getLogger(JsonDataContext.class);

    private final Resource _resource;
    private final SchemaBuilder _schemaBuilder;

    public JsonDataContext(File file) {
        this(new FileResource(file));
    }

    public JsonDataContext(Resource resource) {
        this(resource, new SingleTableInferentialSchemaBuilder(resource));
    }

    public JsonDataContext(Resource resource, SchemaBuilder schemaBuilder) {
        _resource = resource;
        _schemaBuilder = schemaBuilder;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final LazyDocumentSource documentSource = new LazyDocumentSource(new LazyRef<DocumentSource>() {
            @Override
            protected DocumentSource fetch() throws Throwable {
                final JsonDocumentSource jsonDocumentIterator = createJsonDocumentIterator();
                return jsonDocumentIterator;
            }
        });
        try {
            _schemaBuilder.offerSource(documentSource);
            return _schemaBuilder.build();
        } finally {
            documentSource.close();
        }
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return ResourceUtils.getParentName(_resource);
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {

        final DocumentConverter documentConverter = _schemaBuilder.getDocumentConverter(table);
        final SelectItem[] selectItems = MetaModelHelper.createSelectItems(columns);
        final DataSetHeader header = new CachingDataSetHeader(selectItems);
        final JsonDocumentSource documentSource = createJsonDocumentIterator();

        DataSet dataSet = new JsonDataSet(header, documentSource, documentConverter);

        if (maxRows > 0) {
            dataSet = new MaxRowsDataSet(dataSet, maxRows);
        }

        return dataSet;
    }

    private JsonDocumentSource createJsonDocumentIterator() {
        final InputStream inputStream = _resource.read();
        try {
            final MappingJsonFactory jsonFactory = new MappingJsonFactory();
            final JsonParser parser = jsonFactory.createJsonParser(inputStream);
            logger.debug("Created JSON parser for resource: {}", _resource);

            return new JsonDocumentSource(parser);
        } catch (Exception e) {
            FileHelper.safeClose(inputStream);
            throw new MetaModelException("Unexpected error while creating JSON parser", e);
        }
    }
}
