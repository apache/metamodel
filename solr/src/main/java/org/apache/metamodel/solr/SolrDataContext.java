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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.query.FilterClause;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.GroupByItem;
import org.apache.metamodel.query.OrderByItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.query.LogicalOperator;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
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
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.common.SolrDocumentList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

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

public class SolrDataContext extends QueryPostprocessDataContext implements
		DataContext {

	private static final Logger logger = LoggerFactory
			.getLogger(SolrDataContext.class);

	public static final String FIELD_ID = "_id";

	private final SimpleTableDef tableDef;
	private final String indexName;
	private final String url;

	private SolrQuery query = new SolrQuery();

	private static enum queryTypes {
		ROW, FACET
	};

	private static final int MAX_RETRIES = 0;
	private static final int SOCK_TIMEOUT = 1000;
	private static final int CONN_TIMEOUT = 5000;
	private static final int DEFAULT_LIMIT = 1000;

	public SolrDataContext(String url, String indexName) {
		this.url = url;
		this.indexName = indexName;
		this.tableDef = detectSchema(url, indexName);
	}

	private SimpleTableDef detectSchema(String url, String indexName) {
		SimpleTableDef tableDef = null;

		url += "/schema";

		try {
			HttpClient httpclient = new DefaultHttpClient();
			HttpGet request = new HttpGet(url);
			HttpResponse response = httpclient.execute(request);

			InputStreamReader inputStream = new InputStreamReader(response
					.getEntity().getContent());

			ObjectReader reader = new ObjectMapper().reader(HashMap.class);
			Map<String, Object> outerMap = reader.readValue(inputStream);

			Map<String, Object> schemaMap = (Map) outerMap.get("schema");

			if (outerMap != null) {
				List<Map<String, String>> fieldMapList = (List) schemaMap
						.get("fields");
				List<String> columnNames = new ArrayList<String>();
				List<ColumnType> columnTypes = new ArrayList<ColumnType>();

				for (int i = 0; i < fieldMapList.size(); i++) {
					Map<String, String> fieldMap = fieldMapList.get(i);

					columnNames.add(fieldMap.get("name"));
					columnTypes.add(ColumnType.VARCHAR);
				}

				String[] columnNamesArr = new String[columnNames.size()];
				columnNamesArr = columnNames.toArray(columnNamesArr);

				ColumnType[] columnTypesArr = new ColumnType[columnTypes.size()];
				columnTypesArr = columnTypes.toArray(columnTypesArr);

				tableDef = new SimpleTableDef(indexName, columnNamesArr,
						columnTypesArr);
			}
		} catch (IOException e) {
			logger.error("Failed to parse schema", "", e);
		}

		return tableDef;
	}

	@Override
	protected Schema getMainSchema() throws MetaModelException {
		try {
			final MutableSchema theSchema = new MutableSchema(
					getMainSchemaName());

			final MutableTable table = tableDef.toTable().setSchema(theSchema);
			final Column idColumn = table.getColumnByName(FIELD_ID);
			if (idColumn != null && idColumn instanceof MutableColumn) {
				final MutableColumn mutableColumn = (MutableColumn) idColumn;
				mutableColumn.setPrimaryKey(true);
			}
			theSchema.addTable(table);

			return theSchema;
		} catch (Exception e) {
			throw new MetaModelException("Schema retrieval failed " + e);
		}
	}

	@Override
	protected Number executeCountQuery(Table table,
			List<FilterItem> whereItems, boolean functionApproximationAllowed) {
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

	private String processSingleFilterItem(FilterItem filterItem) {
		String clause = "";

		if (filterItem != null) {
			SelectItem item = filterItem.getSelectItem();
			OperatorType operator = filterItem.getOperator();
			Object operand = filterItem.getOperand();

			if (item == null || operator == null || operand == null) {
				clause = " " + filterItem.getExpression();
			} else {
				String columnName = item.getColumn().getName();
				clause = " " + columnName + operator.toSql() + operand;
			}
		}

		return clause;
	}

	private String createSearchClause(FilterItem filterItem, String clause) {
		FilterItem[] filterItems = filterItem.getChildItems();

		if (filterItems != null && filterItems.length > 0) {
			for (FilterItem filterItemInner : filterItems) {
				if (filterItemInner != null) {
					clause += " " + processSingleFilterItem(filterItemInner);
				}

				return createSearchClause(filterItemInner, clause);
			}
		}

		if (filterItem != null) {
			clause += " " + processSingleFilterItem(filterItem);
		}

		return clause;
	}

	@Override
	public DataSet executeQuery(Query q) throws MetaModelException {
		String queryStr = "";
		QueryResponse response = null;

		Schema schema = getMainSchema();
		Table table = schema.getTable(0);
		Column[] columns = table.getColumns();

		boolean selectAll = true;
		List<SelectItem> selectItems = q.getSelectClause().getItems();

		for (SelectItem selectItem : selectItems) {
			if (selectItem.getFunction() != null
					|| selectItem.getColumn() == null) {
				selectAll = false;
				break;
			}
		}

		if (selectItems.size() == 1 && selectAll == false) {
			return super.executeQuery(q);
		}

		FilterClause clause = q.getWhereClause();

		String searchClause = "";

		if (clause != null) {
			List<FilterItem> filterItemList = clause.getItems();

			for (FilterItem filterItem : filterItemList) {
				searchClause += " "
						+ createSearchClause(filterItem, searchClause);
			}

			queryStr = searchClause;
		}

		if (queryStr.isEmpty()) {
			queryStr = "*:*";
		}

		Integer limitObject = q.getMaxRows();
		int limit = DEFAULT_LIMIT;

		if (limitObject != null) {
			limit = (int) limitObject;
		}

		final List<GroupByItem> groupByItems = q.getGroupByClause().getItems();

		if (groupByItems.size() > 0) {
			query.clear();
			query.setQuery(queryStr);
			query.setFacet(true);

			for (int i = 0; i < groupByItems.size(); i++) {
				String facetField = groupByItems.get(i).getSelectItem()
						.getColumn().getName();
				query.addFacetField(facetField);
			}

			setOrder(q, query, queryTypes.FACET);

			query.setFacetLimit(limit);

			try {
				HttpSolrServer server = initSolrServer();
				response = server.query(query);
			} catch (SolrServerException e) {
				logger.error("Server initialization failed", "", e);
				throw new MetaModelException("Server initialization failed "
						+ e);
			}

			List<FacetField> facetFieldsList = response.getFacetFields();

			String facetFieldName = facetFieldsList.get(0).getName();
			
			Column[] facetColumns = { new MutableColumn("count"),
					new MutableColumn(facetFieldName) };
			
			return new SolrDataSet(facetFieldsList, facetColumns);
		} else if (selectAll) {
			response = selectRows(table, q, queryStr, 0, limit);
			SolrDocumentList results = response.getResults();

			return new SolrDataSet(results, columns);
		}

		return super.executeQuery(q);
	}

	@Override
	protected DataSet materializeMainSchemaTable(Table table, Column[] columns,
			int maxRows) {
		QueryResponse response = selectRows(table, null, "*:*", 0, maxRows);
		return new SolrDataSet(response.getResults(), columns);
	}

	@Override
	protected DataSet materializeMainSchemaTable(Table table, Column[] columns,
			int offset, int num) {
		QueryResponse response = selectRows(table, null, "*:*", offset, num);
		return new SolrDataSet(response.getResults(), columns);
	}

	private void setOrder(Query q, SolrQuery query, queryTypes qtype) {
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

			if (qtype == queryTypes.FACET) {
				if (orderColumn == null) {
					if (direction.equalsIgnoreCase("ASC")) {
						throw new UnsupportedOperationException(
								"Ascending sort on count unsupported");
					}
					query.setFacetSort("count");
				} else {
					if (direction.equalsIgnoreCase("DESC")) {
						throw new UnsupportedOperationException(
								"Descending sort on facet unsupported");
					}
					query.setFacetSort("index");
				}
			}
		}
	}

	private HttpSolrServer initSolrServer() {
		HttpSolrServer server = new HttpSolrServer(url);
		server.setMaxRetries(MAX_RETRIES);
		server.setSoTimeout(SOCK_TIMEOUT);
		server.setConnectionTimeout(CONN_TIMEOUT);
		server.setFollowRedirects(false);

		return server;
	}

	private QueryResponse selectRows(Table table, Query q, String queryStr,
			int offset, int num) {
		HttpSolrServer server = initSolrServer();

		query.clear();
		query.setQuery(queryStr);
		query.setStart(offset);
		query.setRows(num);

		if (q != null) {
			setOrder(q, query, queryTypes.ROW);
		}

		QueryResponse response = null;

		try {
			response = server.query(query);
		} catch (SolrServerException e) {
			logger.error("Search query for documents failed", "", e);
			throw new MetaModelException("Query failed " + e);
		}

		return response;
	}

	@Override
	protected String getMainSchemaName() throws MetaModelException {
		return indexName;
	}
}