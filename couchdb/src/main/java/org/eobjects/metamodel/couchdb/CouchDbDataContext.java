/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.couchdb;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.codehaus.jackson.JsonNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.StreamingViewResult;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult.Row;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.MetaModelHelper;
import org.eobjects.metamodel.QueryPostprocessDataContext;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.SimpleTableDef;

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
                        types = EnumSet.noneOf(ColumnType.class);
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
