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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
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
import org.apache.metamodel.util.SimpleTableDef;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataContext implementation for Neo4j
 */
public class Neo4jDataContext extends QueryPostprocessDataContext implements DataContext, DocumentSourceProvider {
    public static final String SCHEMA_NAME = "neo4j";
    public static final int DEFAULT_PORT = 7474;
    
    static final String NEO4J_KEY_METADATA = "metadata";
    static final String NEO4J_KEY_METADATA_TYPE = "type";
    static final String NEO4J_KEY_PROPERTIES = "properties";
    static final String NEO4J_KEY_DATA = "data";
    static final String NEO4J_KEY_ID = "id";
    static final String NEO4J_KEY_RESPONSE_RESULTS = "results";
    static final String NEO4J_KEY_RESPONSE_ROW = "row";
    static final String NEO4J_COLUMN_NAME_ID = "_id";
    static final String NEO4J_COLUMN_NAME_RELATION_PREFIX = "rel_";
    static final String NEO4J_COLUMN_NAME_RELATION_LIST_INDICATOR = "#";

    private static final Logger logger = LoggerFactory.getLogger(Neo4jDataContext.class);

    private final SimpleTableDef[] _tableDefs;
    private final Neo4jRequestWrapper _requestWrapper;
    private final HttpHost _httpHost;
    private String _serviceRoot = "/db/data";

    public Neo4jDataContext(String hostname, int port, String username, String password, SimpleTableDef... tableDefs) {
        super(false);
        _httpHost = new HttpHost(hostname, port);
        final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, username, password, _serviceRoot);
        _tableDefs = tableDefs;
    }

    public Neo4jDataContext(String hostname, int port, String username, String password, String serviceRoot,
            SimpleTableDef... tableDefs) {
        super(false);
        _httpHost = new HttpHost(hostname, port);
        final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, username, password, _serviceRoot);
        _tableDefs = tableDefs;
        _serviceRoot = serviceRoot;
    }

    public Neo4jDataContext(String hostname, int port, String username, String password) {
        super(false);
        _httpHost = new HttpHost(hostname, port);
        final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, username, password, _serviceRoot);
        _tableDefs = detectTableDefs();
    }

    public Neo4jDataContext(String hostname, int port, String username, String password, String serviceRoot) {
        super(false);
        _httpHost = new HttpHost(hostname, port);
        final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, username, password, _serviceRoot);
        _tableDefs = detectTableDefs();
        _serviceRoot = serviceRoot;
    }

    public Neo4jDataContext(String hostname, int port, CloseableHttpClient httpClient) {
        super(false);
        _httpHost = new HttpHost(hostname, port);
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, _serviceRoot);
        _tableDefs = detectTableDefs();
    }

    public Neo4jDataContext(String hostname, int port, CloseableHttpClient httpClient, String serviceRoot) {
        super(false);
        _httpHost = new HttpHost(hostname, port);
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, _serviceRoot);
        _tableDefs = detectTableDefs();
        _serviceRoot = serviceRoot;
    }

    public Neo4jDataContext(String hostname, int port, CloseableHttpClient httpClient, SimpleTableDef... tableDefs) {
        super(false);
        _httpHost = new HttpHost(hostname, port);
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, _serviceRoot);
        _tableDefs = tableDefs;
    }

    public Neo4jDataContext(String hostname, int port, CloseableHttpClient httpClient, String serviceRoot,
            SimpleTableDef... tableDefs) {
        super(false);
        _httpHost = new HttpHost(hostname, port);
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, _serviceRoot);
        _tableDefs = tableDefs;
        _serviceRoot = serviceRoot;
    }

    @Override
    protected String getDefaultSchemaName() throws MetaModelException {
        return SCHEMA_NAME;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        MutableSchema schema = new MutableSchema(getMainSchemaName());
        for (SimpleTableDef tableDef : _tableDefs) {
            MutableTable table = tableDef.toTable().setSchema(schema);
            schema.addTable(table);
        }
        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return SCHEMA_NAME;
    }

    public SimpleTableDef[] detectTableDefs() {
        final List<SimpleTableDef> tableDefs = new ArrayList<>();
        final String labelsJsonString = _requestWrapper.executeRestRequest(new HttpGet(_serviceRoot + "/labels"));
        final JSONArray labelsJsonArray;
        
        try {
            labelsJsonArray = new JSONArray(labelsJsonString);
            
            for (int i = 0; i < labelsJsonArray.length(); i++) {
                final SimpleTableDef tableDefFromLabel = createTableDefFromLabel(labelsJsonArray.getString(i));
                
                if (tableDefFromLabel != null) {
                    tableDefs.add(tableDefFromLabel);
                }
            }
            
            return tableDefs.toArray(new SimpleTableDef[tableDefs.size()]);
        } catch (final JSONException e) {
            logger.error("Error occurred in parsing JSON while detecting the schema: ", e);
            throw new IllegalStateException(e);
        }
    }
    
    private SimpleTableDef createTableDefFromLabel(final String label) throws JSONException {
        final List<JSONObject> nodesPerLabel = getAllNodesPerLabel(label);
        final List<String> propertiesPerLabel = getPropertiesFromLabelNodes(nodesPerLabel);
        final Set<String> relationshipPropertiesPerLabel = new LinkedHashSet<>();

        for (final JSONObject node : nodesPerLabel) {
            final Integer nodeId = (Integer) node.getJSONObject(NEO4J_KEY_METADATA).get(NEO4J_KEY_ID);
            final Set<String> relationshipPropertiesForNode = createRelationshipPropertiesForNode(nodeId);
            relationshipPropertiesPerLabel.addAll(relationshipPropertiesForNode);
        }
        
        propertiesPerLabel.addAll(relationshipPropertiesPerLabel);

        if (nodesPerLabel.isEmpty()) { 
            return null; // Do not add a table if label has no nodes (empty tables are considered non-existent)
        } else {
            final String[] columnNames = propertiesPerLabel.toArray(new String[propertiesPerLabel.size()]);
            final ColumnTypeResolver columnTypeResolver = new ColumnTypeResolver(nodesPerLabel.get(0), columnNames);
            return new SimpleTableDef(label, columnNames, columnTypeResolver.getColumnTypes());
        } 
    }

    private Set<String> createRelationshipPropertiesForNode(final Integer nodeId) throws JSONException {
        final List<JSONObject> relationshipsPerNode = getOutgoingRelationshipsPerNode(nodeId);
        final Set<String> relationshipProperties = new LinkedHashSet<>();

        for (final JSONObject relationship : relationshipsPerNode) {
            // Add the relationship as a column in the table
            final String relationshipName = relationship.getString(NEO4J_KEY_METADATA_TYPE);
            final String relationshipNameProperty = NEO4J_COLUMN_NAME_RELATION_PREFIX + relationshipName;
            relationshipProperties.add(relationshipNameProperty);
            
            // Add all the relationship properties as table columns
            final List<String> propertiesPerRelationship = getAllPropertiesPerRelationship(relationship);
            relationshipProperties.addAll(propertiesPerRelationship);
        }
        
        return relationshipProperties;
    }

    private List<String> getPropertiesFromLabelNodes(final List<JSONObject> nodesPerLabel) {
        final List<String> propertiesPerLabel = new ArrayList<>();
        
        for (final JSONObject node : nodesPerLabel) {
            final List<String> propertiesPerNode = getAllPropertiesPerNode(node);

            for (final String property : propertiesPerNode) {
                if (!propertiesPerLabel.contains(property)) {
                    propertiesPerLabel.add(property);
                }
            }
        }
        
        return propertiesPerLabel;
    }

    private List<String> getAllPropertiesPerRelationship(JSONObject relationship) {
        final List<String> propertyNames = new ArrayList<>();
        try {
            final String relationshipName = NEO4J_COLUMN_NAME_RELATION_PREFIX + relationship
                    .getJSONObject(NEO4J_KEY_METADATA)
                    .getString(NEO4J_KEY_METADATA_TYPE);
            final JSONObject relationshipPropertiesJSONObject = relationship.getJSONObject(NEO4J_KEY_DATA);
            
            if (relationshipPropertiesJSONObject.length() > 0) {
                final JSONArray relationshipPropertiesNamesJSONArray = relationshipPropertiesJSONObject.names();

                for (int i = 0; i < relationshipPropertiesNamesJSONArray.length(); i++) {
                    final String propertyName = relationshipName + NEO4J_COLUMN_NAME_RELATION_LIST_INDICATOR
                            + relationshipPropertiesNamesJSONArray.getString(i);
                    if (!propertyNames.contains(propertyName)) {
                        propertyNames.add(propertyName);
                    }
                }
            }
            return propertyNames;
        } catch (final JSONException e) {
            logger.error("Error occurred in parsing JSON while getting relationship properties: ", e);
            throw new IllegalStateException(e);
        }
    }

    private List<JSONObject> getOutgoingRelationshipsPerNode(final Integer nodeId) {
        final List<JSONObject> outgoingRelationshipsPerNode = new ArrayList<>();
        final String outgoingRelationshipsPerNodeJsonString = _requestWrapper.executeRestRequest(
                new HttpGet(_serviceRoot + "/node/" + nodeId + "/relationships/out"));
        final JSONArray outgoingRelationshipsPerNodeJsonArray;
        
        try {
            outgoingRelationshipsPerNodeJsonArray = new JSONArray(outgoingRelationshipsPerNodeJsonString);
            for (int i = 0; i < outgoingRelationshipsPerNodeJsonArray.length(); i++) {
                final JSONObject relationship = outgoingRelationshipsPerNodeJsonArray.getJSONObject(i);
                if (!outgoingRelationshipsPerNode.contains(relationship)) {
                    outgoingRelationshipsPerNode.add(relationship);
                }
            }
            return outgoingRelationshipsPerNode;
        } catch (final JSONException e) {
            logger.error("Error occurred in parsing JSON while detecting outgoing relationships for node: " + nodeId,
                    e);
            throw new IllegalStateException(e);
        }
    }

    private List<JSONObject> getAllNodesPerLabel(String label) {
        List<JSONObject> allNodesPerLabel = new ArrayList<JSONObject>();

        String allNodesForLabelJsonString = _requestWrapper.executeRestRequest(new HttpGet(_serviceRoot + "/label/"
                + label + "/nodes"));

        JSONArray allNodesForLabelJsonArray;
        try {
            allNodesForLabelJsonArray = new JSONArray(allNodesForLabelJsonString);
            for (int i = 0; i < allNodesForLabelJsonArray.length(); i++) {
                JSONObject node = allNodesForLabelJsonArray.getJSONObject(i);
                allNodesPerLabel.add(node);
            }
            return allNodesPerLabel;
        } catch (JSONException e) {
            logger.error("Error occurred in parsing JSON while detecting the nodes for a label: " + label, e);
            throw new IllegalStateException(e);
        }
    }

    private List<String> getAllPropertiesPerNode(JSONObject node) {
        List<String> properties = new ArrayList<String>();
        properties.add(NEO4J_COLUMN_NAME_ID);

        String propertiesEndpoint;
        try {
            propertiesEndpoint = node.getString(NEO4J_KEY_PROPERTIES);
            String allPropertiesPerNodeJsonString = _requestWrapper.executeRestRequest(new HttpGet(propertiesEndpoint));

            JSONObject allPropertiesPerNodeJsonObject = new JSONObject(allPropertiesPerNodeJsonString);
            for (int j = 0; j < allPropertiesPerNodeJsonObject.length(); j++) {
                JSONArray propertiesJsonArray = allPropertiesPerNodeJsonObject.names();
                for (int k = 0; k < propertiesJsonArray.length(); k++) {
                    String property = propertiesJsonArray.getString(k);
                    properties.add(property);
                }
            }
            return properties;
        } catch (JSONException e) {
            logger.error("Error occurred in parsing JSON while detecting the properties of a node: " + node, e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int firstRow, int maxRows) {
        if ((columns != null) && (columns.size() > 0)) {
            Neo4jDataSet dataSet = null;
            try {
                String selectQuery = Neo4jCypherQueryBuilder.buildSelectQuery(table, columns, firstRow, maxRows);
                String responseJSONString = _requestWrapper.executeCypherQuery(selectQuery);
                JSONObject resultJSONObject = new JSONObject(responseJSONString);
                final List<SelectItem> selectItems = columns.stream().map(SelectItem::new).collect(Collectors.toList());
                dataSet = new Neo4jDataSet(selectItems, resultJSONObject);
            } catch (JSONException e) {
                logger.error("Error occurred in parsing JSON while materializing the schema: ", e);
                throw new IllegalStateException(e);
            }

            return dataSet;
        } else {
            logger.error("Encountered null or empty columns array for materializing main schema table.");
            throw new IllegalArgumentException("Columns cannot be null or empty array");
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        return materializeMainSchemaTable(table, columns, 1, maxRows);
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        String countQuery = Neo4jCypherQueryBuilder.buildCountQuery(table.getName(), whereItems);
        String jsonResponse = _requestWrapper.executeCypherQuery(countQuery);

        JSONObject jsonResponseObject;
        try {
            jsonResponseObject = new JSONObject(jsonResponse);
            JSONArray resultsJSONArray = jsonResponseObject.getJSONArray(NEO4J_KEY_RESPONSE_RESULTS);
            JSONObject resultJSONObject = (JSONObject) resultsJSONArray.get(0);
            JSONArray dataJSONArray = resultJSONObject.getJSONArray(NEO4J_KEY_DATA);
            JSONObject rowJSONObject = (JSONObject) dataJSONArray.get(0);
            JSONArray valueJSONArray = rowJSONObject.getJSONArray(NEO4J_KEY_RESPONSE_ROW);
            Number value = (Number) valueJSONArray.get(0);
            return value;
        } catch (JSONException e) {
            logger.error("Error occurred in parsing JSON response: ", e);
            // Do not throw an exception here. Returning null here will make
            // MetaModel attempt to count records manually and therefore recover
            // from the error.
            return null;
        }
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
