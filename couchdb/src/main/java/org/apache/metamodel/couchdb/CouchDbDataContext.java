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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;
import org.codehaus.jackson.JsonNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.StreamingViewResult;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult.Row;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

/**
 * DataContext implementation for CouchDB
 */
public class CouchDbDataContext extends QueryPostprocessDataContext implements UpdateableDataContext {

    public static final int DEFAULT_PORT = 5984;

    public static final String FIELD_ID = "_id";
    public static final String FIELD_REV = "_rev";

    private static final String SCHEMA_NAME = "CouchDB";

    private final CouchDbInstance _couchDbInstance;
    private final SimpleTableDef[] _tableDefs;

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
        this(couchDbInstance, detectSchema(couchDbInstance));
    }

    public CouchDbDataContext(CouchDbInstance couchDbInstance, SimpleTableDef... tableDefs) {
        // the instance represents a handle to the whole couchdb cluster
        _couchDbInstance = couchDbInstance;
        _tableDefs = tableDefs;
    }

    public static SimpleTableDef[] detectSchema(CouchDbInstance couchDbInstance) {
        final List<SimpleTableDef> tableDefs = new ArrayList<SimpleTableDef>();
        final List<String> databaseNames = couchDbInstance.getAllDatabases();
        for (final String databaseName : databaseNames) {

            if (databaseName.startsWith("_")) {
                // don't add system tables
                continue;
            }

            CouchDbConnector connector = couchDbInstance.createConnector(databaseName, false);

            SimpleTableDef tableDef = detectTable(connector);
            tableDefs.add(tableDef);
        }
        return tableDefs.toArray(new SimpleTableDef[tableDefs.size()]);
    }

    public static SimpleTableDef detectTable(CouchDbConnector connector) {
        final SortedMap<String, Set<ColumnType>> columnsAndTypes = new TreeMap<String, Set<ColumnType>>();

        final StreamingViewResult streamingView = connector.queryForStreamingView(new ViewQuery().allDocs().includeDocs(true)
                .limit(1000));
        try {
            final Iterator<Row> rowIterator = streamingView.iterator();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                JsonNode doc = row.getDocAsNode();

                final Iterator<Entry<String, JsonNode>> fieldIterator = doc.getFields();
                while (fieldIterator.hasNext()) {
                    Entry<String, JsonNode> entry = fieldIterator.next();
                    String key = entry.getKey();

                    Set<ColumnType> types = columnsAndTypes.get(key);

                    if (types == null) {
                        types = new HashSet<ColumnType>();
                        columnsAndTypes.put(key, types);
                    }

                    JsonNode value = entry.getValue();
                    if (value == null || value.isNull()) {
                        // do nothing
                    } else if (value.isTextual()) {
                        types.add(ColumnType.VARCHAR);
                    } else if (value.isArray()) {
                        types.add(ColumnType.LIST);
                    } else if (value.isObject()) {
                        types.add(ColumnType.MAP);
                    } else if (value.isBoolean()) {
                        types.add(ColumnType.BOOLEAN);
                    } else if (value.isInt()) {
                        types.add(ColumnType.INTEGER);
                    } else if (value.isLong()) {
                        types.add(ColumnType.BIGINT);
                    } else if (value.isDouble()) {
                        types.add(ColumnType.DOUBLE);
                    }
                }

            }
        } finally {
            streamingView.close();
        }

        final String[] columnNames = new String[columnsAndTypes.size()];
        final ColumnType[] columnTypes = new ColumnType[columnsAndTypes.size()];

        int i = 0;
        for (Entry<String, Set<ColumnType>> columnAndTypes : columnsAndTypes.entrySet()) {
            final String columnName = columnAndTypes.getKey();
            final Set<ColumnType> columnTypeSet = columnAndTypes.getValue();
            final ColumnType columnType;
            if (columnTypeSet.isEmpty()) {
                columnType = ColumnType.OTHER;
            } else if (columnTypeSet.size() == 1) {
                columnType = columnTypeSet.iterator().next();
            } else {
                // TODO: Select best type?
                columnType = ColumnType.OTHER;
            }
            columnNames[i] = columnName;
            columnTypes[i] = columnType;
            i++;
        }

        final SimpleTableDef tableDef = new SimpleTableDef(connector.getDatabaseName(), columnNames, columnTypes);
        return tableDef;
    }

    public CouchDbInstance getCouchDbInstance() {
        return _couchDbInstance;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final MutableSchema schema = new MutableSchema(SCHEMA_NAME);
        for (final SimpleTableDef tableDef : _tableDefs) {
            final MutableTable table = tableDef.toTable().setSchema(schema);
            CouchDbTableCreationBuilder.addMandatoryColumns(table);
            schema.addTable(table);
        }
        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return SCHEMA_NAME;
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
    public void executeUpdate(UpdateScript script) {
        CouchDbUpdateCallback callback = new CouchDbUpdateCallback(this);
        try {
            script.run(callback);
        } finally {
            callback.close();
        }
    }
}
