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
package org.apache.metamodel.elasticsearch;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.*;
import org.apache.metamodel.util.SimpleTableDef;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.ObjectLookupContainer;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ElasticSearchDataContext extends QueryPostprocessDataContext
        implements DataContext {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchDataContext.class);

    private final Client elasticSearchClient;
    private final SimpleTableDef[] tableDefs;
    private Schema schema;
    private static HashMap<String,String> typeAndIndexes = new HashMap<>();

    public ElasticSearchDataContext(Client client, SimpleTableDef... tableDefs) {
        this.elasticSearchClient = client;
        this.schema = null;
        this.tableDefs = tableDefs;
    }

    public ElasticSearchDataContext(Client client) {
        this(client, detectSchema(client));
    }

    public static SimpleTableDef[] detectSchema(Client client) {
        List<String> indexNames = new ArrayList<>();
        ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().execute().actionGet();
        ImmutableOpenMap<String,IndexMetaData> indexes = clusterStateResponse.getState().getMetaData().getIndices();
        for (ObjectCursor<String> typeCursor : indexes.keys())
            indexNames.add(typeCursor.value);
        List<SimpleTableDef> result = new ArrayList<>();
        for (String indexName : indexNames) {
            ClusterState cs = client.admin().cluster().prepareState().setIndices(indexName).execute().actionGet().getState();
            IndexMetaData imd = cs.getMetaData().index(indexName);
            ImmutableOpenMap<String, MappingMetaData> mappings = imd.getMappings();
            ObjectLookupContainer types = mappings.keys();
            for (Object type: types) {
                String typeName = ((ObjectCursor) type).value.toString();
                typeAndIndexes.put(typeName, indexName);
                try {
                    SimpleTableDef table = detectTable(client, indexName, typeName);
                    result.add(table);
                } catch(Exception e) {}
            }

        }
        SimpleTableDef[] tableDefArray = (SimpleTableDef[]) result.toArray(new SimpleTableDef[result.size()]);
        return tableDefArray;
    }

    public static SimpleTableDef detectTable(Client client, String indexName, String typeName) throws Exception {
        ClusterState cs = client.admin().cluster().prepareState().setIndices(indexName).execute().actionGet().getState();
        IndexMetaData imd = cs.getMetaData().index(indexName);
        MappingMetaData mappingMetaData = imd.mapping(typeName);
        Map<String, Object> mp = mappingMetaData.getSourceAsMap();
        Iterator it = mp.entrySet().iterator();
        Map.Entry pair = (Map.Entry)it.next();
        ElasticSearchMetaData metaData = ElasticSearchMetaDataParser.parse(pair.getValue().toString());
        return new SimpleTableDef(typeName, metaData.getColumnNames(), metaData.getColumnTypes());
    }


    @Override
    protected Schema getMainSchema() throws MetaModelException {
        if (schema == null) {
            MutableSchema theSchema = new MutableSchema(getMainSchemaName());
            for (SimpleTableDef tableDef: tableDefs) {
                MutableTable table = tableDef.toTable().setSchema(theSchema);

                theSchema.addTable(table);
            }

            schema = theSchema;
        }
        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return "ElasticSearchSchema";
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        SearchRequestBuilder requestBuilder = elasticSearchClient.
                prepareSearch(typeAndIndexes.get(table.getName())).
                setTypes(table.getName());
        if (limitMaxRowsIsSet(maxRows)) requestBuilder.setSize(maxRows);
        SearchResponse response = requestBuilder.execute().actionGet();
        return new ElasticSearchDataSet(response, columns, false);
    }

    private boolean limitMaxRowsIsSet(int maxRows) {
        return (maxRows != -1);
    }

/*  TODO: Implements executeQuery method using ElasticSearch API to improve the performance
    @Override
    public DataSet executeQuery(Query query) {
        // Check for queries containing only simple selects and where clauses,
        // or if it is a COUNT(*) query.
        // if from clause only contains a main schema table
        List<FromItem> fromItems = query.getFromClause().getItems();
        if (fromItems.size() == 1 && fromItems.get(0).getTable() != null && fromItems.get(0).getTable().getSchema() == schema) {
            final Table table = fromItems.get(0).getTable();

            // if GROUP BY, HAVING and ORDER BY clauses are not specified
            if (query.getGroupByClause().isEmpty() && query.getHavingClause().isEmpty() && query.getOrderByClause().isEmpty()) {

                final List<FilterItem> whereItems = query.getWhereClause().getItems();

                // if all of the select items are "pure" column selection
                boolean allSelectItemsAreColumns = true;
                List<SelectItem> selectItems = query.getSelectClause().getItems();

                // if it is a
                // "SELECT [columns] FROM [table] WHERE [conditions]"
                // query.
                for (SelectItem selectItem : selectItems) {
                    if (selectItem.getFunction() != null || selectItem.getColumn() == null) {
                        allSelectItemsAreColumns = false;
                        break;
                    }
                }

                if (allSelectItemsAreColumns) {
                    logger.debug("Query can be expressed in full ElasticSearch, no post processing needed.");

                    // prepare for a non-post-processed query
                    Column[] columns = new Column[selectItems.size()];
                    for (int i = 0; i < columns.length; i++) {
                        columns[i] = selectItems.get(i).getColumn();
                    }

                    int firstRow = (query.getFirstRow() == null ? 1 : query.getFirstRow());
                    int maxRows = (query.getMaxRows() == null ? -1 : query.getMaxRows());

                    final DataSet dataSet = materializeMainSchemaTableInternal(table, columns, whereItems, firstRow, maxRows,
                            false);
                    return dataSet;
                }
            }
        }
        logger.debug("Query will be simplified for ElasticSearch and post processed.");
        return super.executeQuery(query);
    }

    private DataSet materializeMainSchemaTableInternal(Table table, Column[] columns, List<FilterItem> whereItems, int firstRow,
                                                       int maxRows, boolean queryPostProcessed) {
        //final SearchRequestBuilder collection = elasticSearchClient.prepareSearch(typeAndIndexes.get(table.getName())).setTypes(table.getName());
        ClusterStateResponse clusterStateResponse = elasticSearchClient.admin().cluster().prepareState().execute().actionGet();
        ImmutableOpenMap<String,IndexMetaData> indexes = clusterStateResponse.getState().getMetaData().getIndices();
        //final SearchRequestBuilder collection = elasticSearchClient.prepareSearch("twitter").setTypes("tweet1");
        SearchRequestBuilder requestBuilder = elasticSearchClient.prepareSearch();

        if (whereItems != null && !whereItems.isEmpty()) {
            for (FilterItem item : whereItems) {
                String operandWithIndexName = item.getSelectItem().toString();
                int operandNameIndexStart = operandWithIndexName.indexOf(".")+1;
                String operandWithoutIndexName = operandWithIndexName.substring(operandNameIndexStart);
                requestBuilder.setQuery(QueryBuilders.termQuery(operandWithoutIndexName, item.getOperand()));
            }
        }

        SearchResponse response = requestBuilder.execute().actionGet();

        return new ElasticSearchDataSet(response, columns, queryPostProcessed);
    }*/
}
