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
package org.apache.metamodel.neo4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DocumentSource;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.builder.DocumentSourceProvider;
import org.apache.metamodel.schema.builder.SchemaBuilder;
import org.apache.metamodel.util.SimpleTableDef;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

/**
 * DataContext implementation for Neo4j
 */
public class Neo4jDataContext extends QueryPostprocessDataContext implements UpdateableDataContext,
        DocumentSourceProvider {

    public static final String SCHEMA_NAME = "neo4j";

    public static final int DEFAULT_PORT = 7474;

    private static final String NEO4J_DRIVER_CLASS = "org.neo4j.jdbc.Driver";

    private Connection _connection;

    private SchemaBuilder _schemaBuilder;

    public Neo4jDataContext() {
        createConnection();

        _schemaBuilder = new Neo4jInferentialSchemaBuilder();
    }

    public Neo4jDataContext(Connection connection) {
        _connection = connection;

        _schemaBuilder = new Neo4jInferentialSchemaBuilder();
    }

    private void createConnection() {
        try {
            Class.forName(NEO4J_DRIVER_CLASS);
            _connection = DriverManager.getConnection("jdbc:neo4j://localhost:" + DEFAULT_PORT);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    protected String getDefaultSchemaName() throws MetaModelException {
        return SCHEMA_NAME;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        SimpleTableDef[] tableDefs = detectSchema();
        MutableSchema schema = new MutableSchema(getMainSchemaName());
        for (SimpleTableDef tableDef : tableDefs) {
            MutableTable table = tableDef.toTable().setSchema(schema);
            schema.addTable(table);
        }
        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return SCHEMA_NAME;
    }

    public SimpleTableDef[] detectSchema() {
        Collection<String> finalLabels = new ArrayList<String>();

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = _connection.createStatement();
            resultSet = statement.executeQuery("START n=node(*) RETURN distinct labels(n)");
            while (resultSet.next()) {
                String labelList = resultSet.getString("labels(n)");
                if (labelList.contains(",")) {
                    labelList = labelList.substring(1, labelList.length() - 1);
                    Iterable<String> labelsSplitted = Splitter.on(",").trimResults(CharMatcher.is('\"')).split(labelList);
                    for (String label : labelsSplitted) {
                        if (!finalLabels.contains(label)) {
                            finalLabels.add(label);
                        }
                    }
                } else {
                    labelList = labelList.substring(2, labelList.length() - 2);
                    if (!finalLabels.contains(labelList)) {
                        finalLabels.add(labelList);
                    }
                }
            }
            SimpleTableDef[] result = new SimpleTableDef[finalLabels.size()];
            int i = 0;
            for (String label : finalLabels) {
                // TODO: Column names set to NULL...
                SimpleTableDef table = new SimpleTableDef(label, null);
                result[i] = table;
                i++;
            }
            return result;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int firstRow, int maxRows) {
        final String label = table.getName();

        Statement statement = null;
        try {
            statement = _connection.createStatement();
            ResultSet resultSet = statement.executeQuery("MATCH (n:" + label + ") RETURN n");
            return new Neo4jDataSet(null, this, _connection, statement, resultSet);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        return materializeMainSchemaTable(table, columns, 1, maxRows);
    }

    @Override
    protected org.apache.metamodel.data.Row executePrimaryKeyLookupQuery(Table table, List<SelectItem> selectItems,
            Column primaryKeyColumn, Object keyValue) {
        // if (keyValue == null) {
        // return null;
        // }
        //
        // final String databaseName = table.getName();
        // final CouchDbConnector connector =
        // _couchDbInstance.createConnector(databaseName, false);
        //
        // final String keyString = keyValue.toString();
        // final JsonNode node = connector.find(JsonNode.class, keyString);
        // if (node == null) {
        // return null;
        // }
        //
        // return CouchDbUtils.jsonNodeToMetaModelRow(node, new
        // SimpleDataSetHeader(selectItems));
        return null;
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        // if (whereItems.isEmpty()) {
        // String databaseName = table.getName();
        // CouchDbConnector connector =
        // _couchDbInstance.createConnector(databaseName, false);
        // long docCount = connector.getDbInfo().getDocCount();
        // return docCount;
        // }
        // return null;
        return -1;
    }

    @Override
    public void executeUpdate(UpdateScript script) {
        // CouchDbUpdateCallback callback = new CouchDbUpdateCallback(this);
        // try {
        // script.run(callback);
        // } finally {
        // callback.close();
        // }
    }

    @Override
    public DocumentSource getMixedDocumentSourceForSampling() {
        return null;
    }

    @Override
    public DocumentSource getDocumentSourceForTable(String sourceCollectionName) {
        return null;
    }
}
