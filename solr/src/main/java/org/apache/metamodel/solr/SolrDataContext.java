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
package org.apache.metamodel.solr;

import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.OrderByItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * DataContext implementation for Solr analytics engine.
 *
 * Solr has a data storage structure hierarchy that briefly goes like
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

public class SolrDataContext extends QueryPostprocessDataContext implements
        DataContext {

    private static final Logger logger = LoggerFactory
            .getLogger(SolrDataContext.class);

    private final SimpleTableDef _tableDef;
    private final String _indexName;
    private final String _url;

    private SolrQuery _query = new SolrQuery();

    private static enum QueryType {
        ROW, FACET
    };

    private static final int MAX_RETRIES = 0;
    private static final int SOCK_TIMEOUT = 1000;
    private static final int CONN_TIMEOUT = 5000;
    private static final int DEFAULT_LIMIT = 100;

    /**
     * Constructor to create DataContext object.
     * @param url an absolute url referencing a particular Solr core
     * @param indexName name of the referenced core
    **/
    public SolrDataContext(final String url, final String indexName) {
        this._url = url;
        this._indexName = indexName;
        this._tableDef = detectSchema();
    }

    /**
     * Returns tableDef object containing the table definition after parsing the schema from the REST service.
     * @param None
     * @return the tableDef object
    **/
    private SimpleTableDef detectSchema() {
        SimpleTableDef tableDef = null;

        final String schemaUrl = _url + "/schema";

        try {
            HttpClient httpclient = new DefaultHttpClient();
            HttpGet request = new HttpGet(schemaUrl);
            HttpResponse response = httpclient.execute(request);

            InputStreamReader inputStream = new InputStreamReader(response
                    .getEntity().getContent());

            ObjectReader reader = new ObjectMapper().reader(HashMap.class);
            Map<String, Object> outerMap = reader.readValue(inputStream);

            Map<String, Object> schemaMap = null;

            if (outerMap != null) {
                schemaMap = (Map<String, Object>) outerMap.get("schema");
            }

            if (schemaMap != null) {
                List<Map<String, String>> fieldMapList = (List<Map<String, String>>) schemaMap
                        .get("fields");
                List<String> columnNames = new ArrayList<String>();
                List<ColumnType> columnTypes = new ArrayList<ColumnType>();

                for (int i = 0; i < fieldMapList.size(); i++) {
                    Map<String, String> fieldMap = fieldMapList.get(i);

                    columnNames.add(fieldMap.get("name"));
                    columnTypes.add(ColumnType.STRING);
                }

                String[] columnNamesArr = new String[columnNames.size()];
                columnNamesArr = columnNames.toArray(columnNamesArr);

                ColumnType[] columnTypesArr = new ColumnType[columnTypes.size()];
                columnTypesArr = columnTypes.toArray(columnTypesArr);

                tableDef = new SimpleTableDef(_indexName, columnNamesArr,
                        columnTypesArr);
            }
        } catch (IOException e) {
            logger.error("Failed to parse schema", "", e);
        }

        return tableDef;
    }

    @Override
    final protected Schema getMainSchema() throws MetaModelException {
        try {
            final MutableSchema theSchema = new MutableSchema(
                    getMainSchemaName());

            final MutableTable table = _tableDef.toTable().setSchema(theSchema);
            theSchema.addTable(table);

            return theSchema;
        } catch (Exception e) {
            throw new MetaModelException("Schema retrieval failed " + e);
        }
    }

    @Override
    final protected Number executeCountQuery(final Table table,
            final List<FilterItem> whereItems, final boolean functionApproximationAllowed) {
        String queryStr = "*:*";

        if (whereItems != null) {
            for (int i = 0; i < whereItems.size(); i++) {
                FilterItem whereItem = whereItems.get(i);
                queryStr = whereItem.toString();
            }
        }

        QueryResponse response = selectRows(table, null, queryStr, 0, 0);
        return response.getResults().getNumFound();
    }

    @Override
    final protected DataSet materializeMainSchemaTable(final Table table, final Column[] columns,
            final int maxRows) {
        QueryResponse response = selectRows(table, null, "*:*", 0, maxRows);
        return new SolrDataSet(response.getResults(), columns);
    }

    @Override
    final protected DataSet materializeMainSchemaTable(final Table table, final Column[] columns,
            final int offset, final int num) {
        int limit = num;

        if (num == -1) {
            limit = this.executeCountQuery(table, null, false).intValue();
        }

        if (DEFAULT_LIMIT > limit) {
            QueryResponse response = selectRows(table, null, "*:*", offset, limit);
            return new SolrDataSet(response.getResults(), columns);
        } else {
            HttpSolrServer server = initSolrServer();
            return new SolrDataSet(server, "*:*", columns, limit);
        }
    }

    /**
     * Sets the equivalent of SQL 'order by' clause in the Solr query.
     * @param q Query object of MM
     * @param query SolrJ Query object
     * @param qtype Enum to indicate row or facet query
    **/
    private void setOrder(final Query q, final SolrQuery query, final QueryType qtype) {
        List<OrderByItem> orderByList = q.getOrderByClause().getItems();

        for (OrderByItem orderItem : orderByList) {
            SelectItem orderFieldOrFunc = orderItem.getSelectItem();
            Column orderColumn = orderFieldOrFunc.getColumn();
            FunctionType orderFunc = orderFieldOrFunc.getFunction();

            String orderField = null;
            String direction = orderItem.getDirection().toString();

            if (orderColumn != null) {
                orderField = orderColumn.getName();

                if (direction == null || direction.equalsIgnoreCase("ASC")) {
                    query.addSort(orderField, SolrQuery.ORDER.asc);
                } else {
                    query.addSort(orderField, SolrQuery.ORDER.desc);
                }
            }

            if (qtype == QueryType.FACET) {
                if (orderColumn == null) {
                    if (!direction.equalsIgnoreCase("ASC")) {
                        query.setFacetSort("count");
                    }
                } else {
                    if (!direction.equalsIgnoreCase("DESC")) {
                        query.setFacetSort("index");
                    }
                }
            }
        }
    }

    /**
     * Initialize connection to the Solr server based on the url.
     * @return server object
    **/
    private HttpSolrServer initSolrServer() {
        HttpSolrServer server = new HttpSolrServer(_url);
        server.setMaxRetries(MAX_RETRIES);
        server.setSoTimeout(SOCK_TIMEOUT);
        server.setConnectionTimeout(CONN_TIMEOUT);
        server.setFollowRedirects(false);

        return server;
    }

    /**
     * Select rows/documents from the index.
     * @param table Table object representing the core/schema
     * @param q Query object of MM framework
     * @param queryStr String representation of the query/keyword
     * @param offset Offset for pagination
     * @param num  Number of rows to retrieve
     * @return QueryResponse SolrJ response object
    **/
    private QueryResponse selectRows(final Table table, final Query q, final String queryStr,
            final int offset, final int num) {
        HttpSolrServer server = initSolrServer();

        _query.clear();
        _query.setQuery(queryStr);
        _query.setStart(offset);
        _query.setRows(num);

        if (q != null) {
            setOrder(q, _query, QueryType.ROW);
        }

        QueryResponse response = null;

        try {
            response = server.query(_query);
        } catch (SolrServerException e) {
            logger.error("Search query for documents failed", "", e);
            throw new MetaModelException("Query failed " + e);
        }

        return response;
    }

    @Override
    final protected String getMainSchemaName() throws MetaModelException {
        return _indexName;
    }
}
