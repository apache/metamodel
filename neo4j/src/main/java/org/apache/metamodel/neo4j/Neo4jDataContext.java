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

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.MetaModelHelper;
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

    public static final Logger logger = LoggerFactory.getLogger(Neo4jDataContext.class);

    public static final String SCHEMA_NAME = "neo4j";

    public static final int DEFAULT_PORT = 7474;

    public static final String RELATIONSHIP_PREFIX = "rel_";

    public static final String RELATIONSHIP_COLUMN_SEPARATOR = "#";

    private final SimpleTableDef[] _tableDefs;

    private final Neo4jRequestWrapper _requestWrapper;

    private final HttpHost _httpHost;

    private String _serviceRoot = "/db/data";

    public Neo4jDataContext(String hostname, int port, String username, String password, SimpleTableDef... tableDefs) {
        _httpHost = new HttpHost(hostname, port);
        final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, username, password, _serviceRoot);
        _tableDefs = tableDefs;
    }

    public Neo4jDataContext(String hostname, int port, String username, String password, String serviceRoot,
            SimpleTableDef... tableDefs) {
        _httpHost = new HttpHost(hostname, port);
        final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, username, password, _serviceRoot);
        _tableDefs = tableDefs;
        _serviceRoot = serviceRoot;
    }

    public Neo4jDataContext(String hostname, int port, String username, String password) {
        _httpHost = new HttpHost(hostname, port);
        final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, username, password, _serviceRoot);
        _tableDefs = detectTableDefs();
    }

    public Neo4jDataContext(String hostname, int port, String username, String password, String serviceRoot) {
        _httpHost = new HttpHost(hostname, port);
        final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, username, password, _serviceRoot);
        _tableDefs = detectTableDefs();
        _serviceRoot = serviceRoot;
    }

    public Neo4jDataContext(String hostname, int port, CloseableHttpClient httpClient) {
        _httpHost = new HttpHost(hostname, port);
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, _serviceRoot);
        _tableDefs = detectTableDefs();
    }

    public Neo4jDataContext(String hostname, int port, CloseableHttpClient httpClient, String serviceRoot) {
        _httpHost = new HttpHost(hostname, port);
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, _serviceRoot);
        _tableDefs = detectTableDefs();
        _serviceRoot = serviceRoot;
    }

    public Neo4jDataContext(String hostname, int port, CloseableHttpClient httpClient, SimpleTableDef... tableDefs) {
        _httpHost = new HttpHost(hostname, port);
        _requestWrapper = new Neo4jRequestWrapper(httpClient, _httpHost, _serviceRoot);
        _tableDefs = tableDefs;
    }

    public Neo4jDataContext(String hostname, int port, CloseableHttpClient httpClient, String serviceRoot,
            SimpleTableDef... tableDefs) {
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
        List<SimpleTableDef> tableDefs = new ArrayList<SimpleTableDef>();

        String labelsJsonString = _requestWrapper.executeRestRequest(new HttpGet(_serviceRoot + "/labels"));

        JSONArray labelsJsonArray;
        try {
            labelsJsonArray = new JSONArray(labelsJsonString);
            for (int i = 0; i < labelsJsonArray.length(); i++) {
                String label = labelsJsonArray.getString(i);

                List<JSONObject> nodesPerLabel = getAllNodesPerLabel(label);

                List<String> propertiesPerLabel = new ArrayList<String>();
                for (JSONObject node : nodesPerLabel) {
                    List<String> propertiesPerNode = getAllPropertiesPerNode(node);
                    for (String property : propertiesPerNode) {
                        if (!propertiesPerLabel.contains(property)) {
                            propertiesPerLabel.add(property);
                        }
                    }
                }

                Set<String> relationshipPropertiesPerLabel = new LinkedHashSet<String>();
                for (JSONObject node : nodesPerLabel) {
                    Integer nodeId = (Integer) node.getJSONObject("metadata").get("id");
                    List<JSONObject> relationshipsPerNode = getOutgoingRelationshipsPerNode(nodeId);
                    for (JSONObject relationship : relationshipsPerNode) {
                        // Add the relationship as a column in the table
                        String relationshipName = relationship.getString("type");
                        String relationshipNameProperty = RELATIONSHIP_PREFIX + relationshipName;
                        if (!relationshipPropertiesPerLabel.contains(relationshipNameProperty)) {
                            relationshipPropertiesPerLabel.add(relationshipNameProperty);
                        }

                        // Add all the relationship properties as table columns
                        List<String> propertiesPerRelationship = getAllPropertiesPerRelationship(relationship);
                        relationshipPropertiesPerLabel.addAll(propertiesPerRelationship);
                    }
                }
                propertiesPerLabel.addAll(relationshipPropertiesPerLabel);

                // Do not add a table if label has no nodes (empty tables are
                // considered non-existent)
                if (!nodesPerLabel.isEmpty()) {
                    SimpleTableDef tableDef = new SimpleTableDef(label,
                            propertiesPerLabel.toArray(new String[propertiesPerLabel.size()]));
                    tableDefs.add(tableDef);
                }
            }
            return tableDefs.toArray(new SimpleTableDef[tableDefs.size()]);
        } catch (JSONException e) {
            logger.error("Error occured in parsing JSON while detecting the schema: ", e);
            throw new IllegalStateException(e);
        }
    }

    private List<String> getAllPropertiesPerRelationship(JSONObject relationship) {
        List<String> propertyNames = new ArrayList<String>();
        try {
            String relationshipName = RELATIONSHIP_PREFIX + relationship.getJSONObject("metadata").getString("type");
            JSONObject relationshipPropertiesJSONObject = relationship.getJSONObject("data");
            if (relationshipPropertiesJSONObject.length() > 0) {
                JSONArray relationshipPropertiesNamesJSONArray = relationshipPropertiesJSONObject.names();
                for (int i = 0; i < relationshipPropertiesNamesJSONArray.length(); i++) {
                    String propertyName = relationshipName + RELATIONSHIP_COLUMN_SEPARATOR
                            + relationshipPropertiesNamesJSONArray.getString(i);
                    if (!propertyNames.contains(propertyName)) {
                        propertyNames.add(propertyName);
                    }
                }
            }
            return propertyNames;
        } catch (JSONException e) {
            logger.error("Error occured in parsing JSON while getting relationship properties: ", e);
            throw new IllegalStateException(e);
        }
    }

    private List<JSONObject> getOutgoingRelationshipsPerNode(Integer nodeId) {
        List<JSONObject> outgoingRelationshipsPerNode = new ArrayList<JSONObject>();

        String outgoingRelationshipsPerNodeJsonString = _requestWrapper.executeRestRequest(new HttpGet(_serviceRoot
                + "/node/" + nodeId + "/relationships/out"));

        JSONArray outgoingRelationshipsPerNodeJsonArray;
        try {
            outgoingRelationshipsPerNodeJsonArray = new JSONArray(outgoingRelationshipsPerNodeJsonString);
            for (int i = 0; i < outgoingRelationshipsPerNodeJsonArray.length(); i++) {
                JSONObject relationship = outgoingRelationshipsPerNodeJsonArray.getJSONObject(i);
                if (!outgoingRelationshipsPerNode.contains(relationship)) {
                    outgoingRelationshipsPerNode.add(relationship);
                }
            }
            return outgoingRelationshipsPerNode;
        } catch (JSONException e) {
            logger.error("Error occured in parsing JSON while detecting outgoing relationships for node: " + nodeId, e);
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
            logger.error("Error occured in parsing JSON while detecting the nodes for a label: " + label, e);
            throw new IllegalStateException(e);
        }
    }

    private List<String> getAllPropertiesPerNode(JSONObject node) {
        List<String> properties = new ArrayList<String>();
        properties.add("_id");

        String propertiesEndpoint;
        try {
            propertiesEndpoint = node.getString("properties");

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
            logger.error("Error occured in parsing JSON while detecting the properties of a node: " + node, e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int firstRow, int maxRows) {
        if ((columns != null) && (columns.length > 0)) {
            Neo4jDataSet dataSet = null;
            try {
                String selectQuery = Neo4jCypherQueryBuilder.buildSelectQuery(table, columns, firstRow, maxRows);
                String responseJSONString = _requestWrapper.executeCypherQuery(selectQuery);
                JSONObject resultJSONObject = new JSONObject(responseJSONString);
                final SelectItem[] selectItems = MetaModelHelper.createSelectItems(columns);
                dataSet = new Neo4jDataSet(selectItems, resultJSONObject);
            } catch (JSONException e) {
                logger.error("Error occured in parsing JSON while materializing the schema: ", e);
                throw new IllegalStateException(e);
            }

            return dataSet;
        } else {
            logger.error("Encountered null or empty columns array for materializing main schema table.");
            throw new IllegalArgumentException("Columns cannot be null or empty array");
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        return materializeMainSchemaTable(table, columns, 1, maxRows);
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        String countQuery = Neo4jCypherQueryBuilder.buildCountQuery(table.getName(), whereItems);
        String jsonResponse = _requestWrapper.executeCypherQuery(countQuery);

        JSONObject jsonResponseObject;
        try {
            jsonResponseObject = new JSONObject(jsonResponse);
            JSONArray resultsJSONArray = jsonResponseObject.getJSONArray("results");
            JSONObject resultJSONObject = (JSONObject) resultsJSONArray.get(0);
            JSONArray dataJSONArray = resultJSONObject.getJSONArray("data");
            JSONObject rowJSONObject = (JSONObject) dataJSONArray.get(0);
            JSONArray valueJSONArray = rowJSONObject.getJSONArray("row");
            Number value = (Number) valueJSONArray.get(0);
            return value;
        } catch (JSONException e) {
            logger.error("Error occured in parsing JSON response: ", e);
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
