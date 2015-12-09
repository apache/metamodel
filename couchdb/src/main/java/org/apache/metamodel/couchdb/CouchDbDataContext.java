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

import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateSummary;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DocumentSource;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.builder.DocumentSourceProvider;
import org.apache.metamodel.schema.builder.SchemaBuilder;
import org.apache.metamodel.util.SimpleTableDef;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.StreamingViewResult;
import org.ektorp.ViewQuery;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * DataContext implementation for CouchDB
 */
public class CouchDbDataContext extends QueryPostprocessDataContext implements UpdateableDataContext,
        DocumentSourceProvider {

    public static final String SCHEMA_NAME = "CouchDB";

    public static final int DEFAULT_PORT = 5984;

    public static final String FIELD_ID = "_id";
    public static final String FIELD_REV = "_rev";

    // the instance represents a handle to the whole couchdb cluster
    private final CouchDbInstance _couchDbInstance;
    private final SchemaBuilder _schemaBuilder;

    public CouchDbDataContext(StdHttpClient.Builder httpClientBuilder, SimpleTableDef... tableDefs) {
        this(httpClientBuilder.build(), tableDefs);
    }

    public CouchDbDataContext(StdHttpClient.Builder httpClientBuilder) {
        this(httpClientBuilder.build());
    }

    public CouchDbDataContext(HttpClient httpClient, SimpleTableDef... tableDefs) {
        this(new StdCouchDbInstance(httpClient), tableDefs);
    }

    public CouchDbDataContext(HttpClient httpClient) {
        this(new StdCouchDbInstance(httpClient));
    }

    public CouchDbDataContext(CouchDbInstance couchDbInstance) {
        _couchDbInstance = couchDbInstance;
        _schemaBuilder = new CouchDbInferentialSchemaBuilder();
    }

    public CouchDbDataContext(CouchDbInstance couchDbInstance, String... databaseNames) {
        _couchDbInstance = couchDbInstance;
        _schemaBuilder = new CouchDbInferentialSchemaBuilder(databaseNames);
    }

    public CouchDbDataContext(CouchDbInstance couchDbInstance, SimpleTableDef... tableDefs) {
        _couchDbInstance = couchDbInstance;
        _schemaBuilder = new CouchDbSimpleTableDefSchemaBuilder(tableDefs);
    }

    public CouchDbInstance getCouchDbInstance() {
        return _couchDbInstance;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        _schemaBuilder.offerSources(this);
        return _schemaBuilder.build();
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _schemaBuilder.getSchemaName();
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int firstRow, int maxRows) {
        // the connector represents a handle to the the couchdb "database".
        final String databaseName = table.getName();
        final CouchDbConnector connector = _couchDbInstance.createConnector(databaseName, false);

        ViewQuery query = new ViewQuery().allDocs().includeDocs(true);

        if (maxRows > 0) {
            query = query.limit(maxRows);
        }
        if (firstRow > 1) {
            final int skip = firstRow - 1;
            query = query.skip(skip);
        }

        final StreamingViewResult streamingView = connector.queryForStreamingView(query);

        final SelectItem[] selectItems = MetaModelHelper.createSelectItems(columns);
        return new CouchDbDataSet(selectItems, streamingView);
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        return materializeMainSchemaTable(table, columns, 1, maxRows);
    }

    @Override
    protected org.apache.metamodel.data.Row executePrimaryKeyLookupQuery(Table table, List<SelectItem> selectItems,
            Column primaryKeyColumn, Object keyValue) {
        if (keyValue == null) {
            return null;
        }

        final String databaseName = table.getName();
        final CouchDbConnector connector = _couchDbInstance.createConnector(databaseName, false);

        final String keyString = keyValue.toString();
        final JsonNode node = connector.find(JsonNode.class, keyString);
        if (node == null) {
            return null;
        }

        return CouchDbUtils.jsonNodeToMetaModelRow(node, new SimpleDataSetHeader(selectItems));
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        if (whereItems.isEmpty()) {
            String databaseName = table.getName();
            CouchDbConnector connector = _couchDbInstance.createConnector(databaseName, false);
            long docCount = connector.getDbInfo().getDocCount();
            return docCount;
        }
        return null;
    }

    @Override
    public UpdateSummary executeUpdate(UpdateScript script) {
        final CouchDbUpdateCallback callback = new CouchDbUpdateCallback(this);
        try {
            script.run(callback);
        } finally {
            callback.close();
        }
        return callback.getUpdateSummary();
    }

    @Override
    public DocumentSource getMixedDocumentSourceForSampling() {
        return new CouchDbSamplingDocumentSource(_couchDbInstance);
    }

    @Override
    public DocumentSource getDocumentSourceForTable(String sourceCollectionName) {
        return new CouchDbDatabaseDocumentSource(_couchDbInstance, sourceCollectionName, -1);
    }
}
