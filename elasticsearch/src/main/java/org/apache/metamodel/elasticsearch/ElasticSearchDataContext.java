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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.ObjectLookupContainer;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataContext implementation for ElasticSearch analytics engine.
 * 
 * ElasticSearch has a data storage structure hierarchy that briefly goes like
 * this:
 * <ul>
 * <li>Index</li>
 * <li>Document type (short: Type) (within an index)</li>
 * <li>Documents (of a particular type)</li>
 * </ul>
 * 
 * When instantiating this DataContext, an index name is provided. Within this
 * index, each document type is represented as a table.
 * 
 * This implementation supports either automatic discovery of a schema or manual
 * specification of a schema, through the {@link SimpleTableDef} class.
 */
public class ElasticSearchDataContext extends QueryPostprocessDataContext implements DataContext {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchDataContext.class);

    private final Client elasticSearchClient;
    private final SimpleTableDef[] tableDefs;
    private final String indexName;

    /**
     * Constructs a {@link ElasticSearchDataContext}. This constructor accepts a
     * custom array of {@link SimpleTableDef}s which allows the user to define
     * his own view on the indexes in the engine.
     *
     * @param client
     *            the ElasticSearch client
     * @param indexName
     *            the name of the ElasticSearch index to represent
     * @param tableDefs
     *            an array of {@link SimpleTableDef}s, which define the table
     *            and column model of the ElasticSearch index.
     */
    public ElasticSearchDataContext(Client client, String indexName, SimpleTableDef... tableDefs) {
        this.elasticSearchClient = client;
        this.indexName = indexName;
        this.tableDefs = tableDefs;
    }

    /**
     * Constructs a {@link ElasticSearchDataContext} and automatically detects
     * the schema structure/view on all indexes (see
     * {@link #detectSchema(Client)}).
     *
     * @param client
     *            the ElasticSearch client
     * @param indexName
     *            the name of the ElasticSearch index to represent
     */
    public ElasticSearchDataContext(Client client, String indexName) {
        this(client, indexName, detectSchema(client, indexName));
    }

    /**
     * Performs an analysis of the available indexes in an ElasticSearch cluster
     * {@link Client} instance and detects the elasticsearch types structure
     * based on the metadata provided by the ElasticSearch java client.
     *
     * @see #detectTable(ClusterState, String, String)
     *
     * @param client
     *            the client to inspect
     * @param indexName2
     * @return a mutable schema instance, useful for further fine tuning by the
     *         user.
     */
    public static SimpleTableDef[] detectSchema(Client client, String indexName) {
        final ClusterState cs;
        final ClusterStateRequestBuilder clusterStateRequestBuilder = client.admin().cluster().prepareState();

        // different methods here to set the index name, so we have to use
        // reflection :-/
        try {
            final byte majorVersion = Version.CURRENT.major;
            final Object methodArgument = new String[] { indexName };
            if (majorVersion == 0) {
                final Method method = ClusterStateRequestBuilder.class.getMethod("setFilterIndices", String[].class);
                method.invoke(clusterStateRequestBuilder, methodArgument);
            } else {
                final Method method = ClusterStateRequestBuilder.class.getMethod("setIndices", String[].class);
                method.invoke(clusterStateRequestBuilder, methodArgument);
            }
        } catch (Exception e) {
            logger.error("Failed to set index name on ClusterStateRequestBuilder, version {}", Version.CURRENT, e);
            throw new MetaModelException("Failed to create request for index information needed to detect schema", e);
        }
        cs = clusterStateRequestBuilder.execute().actionGet().getState();

        final IndexMetaData imd = cs.getMetaData().index(indexName);
        final ImmutableOpenMap<String, MappingMetaData> mappings = imd.getMappings();
        final ObjectLookupContainer<String> documentTypes = mappings.keys();

        final List<SimpleTableDef> result = new ArrayList<SimpleTableDef>();
        for (final Object documentTypeCursor : documentTypes) {
            final String documentType = ((ObjectCursor<?>) documentTypeCursor).value.toString();
            try {
                final SimpleTableDef table = detectTable(cs, indexName, documentType);
                result.add(table);
            } catch (Exception e) {
                logger.error("Unexpected error during detectTable for document type: {}", documentType, e);
            }
        }
        final SimpleTableDef[] tableDefArray = (SimpleTableDef[]) result.toArray(new SimpleTableDef[result.size()]);
        Arrays.sort(tableDefArray, new Comparator<SimpleTableDef>() {
            @Override
            public int compare(SimpleTableDef o1, SimpleTableDef o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        return tableDefArray;
    }

    /**
     * Performs an analysis of an available index type in an ElasticSearch
     * {@link Client} client and tries to detect the index structure based on
     * the metadata provided by the java client.
     *
     * @param cs
     *            the ElasticSearch cluster
     * @param indexName
     *            the name of the index
     * @param documentType
     *            the name of the index type
     * @return a table definition for ElasticSearch.
     */
    public static SimpleTableDef detectTable(ClusterState cs, String indexName, String documentType) throws Exception {
        final IndexMetaData imd = cs.getMetaData().index(indexName);
        final MappingMetaData mappingMetaData = imd.mapping(documentType);
        final Map<String, Object> mp = mappingMetaData.getSourceAsMap();
        final Iterator<Map.Entry<String, Object>> it = mp.entrySet().iterator();
        final Map.Entry<String, Object> pair = it.next();
        final ElasticSearchMetaData metaData = ElasticSearchMetaDataParser.parse(pair.getValue());
        return new SimpleTableDef(documentType, metaData.getColumnNames(), metaData.getColumnTypes());
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final MutableSchema theSchema = new MutableSchema(getMainSchemaName());
        for (final SimpleTableDef tableDef : tableDefs) {
            final MutableTable table = tableDef.toTable().setSchema(theSchema);
            theSchema.addTable(table);
        }
        return theSchema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return indexName;
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        final String documentType = table.getName();
        final SearchRequestBuilder requestBuilder = elasticSearchClient.prepareSearch(indexName).setTypes(documentType);
        if (limitMaxRowsIsSet(maxRows)) {
            requestBuilder.setSize(maxRows);
        }
        final SearchResponse response = requestBuilder.execute().actionGet();
        return new ElasticSearchDataSet(response, columns, false);
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        if (!whereItems.isEmpty()) {
            // not supported - will have to be done by counting client-side
            return null;
        }
        String documentType = table.getName();
        final CountResponse response = elasticSearchClient.prepareCount(indexName)
                .setQuery(QueryBuilders.termQuery("_type", documentType)).execute().actionGet();
        return response.getCount();
    }

    private boolean limitMaxRowsIsSet(int maxRows) {
        return (maxRows != -1);
    }
}
