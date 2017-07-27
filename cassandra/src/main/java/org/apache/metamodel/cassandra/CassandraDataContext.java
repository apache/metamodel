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
package org.apache.metamodel.cassandra;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;

/**
 * DataContext implementation for Apache Cassandra database.
 *
 * When instantiating this DataContext, a keyspace name is provided. In
 * Cassandra, the keyspace is the container for your application data, similar
 * to a schema in a relational database. Keyspaces are used to group column
 * families together.
 * 
 * This implementation supports either automatic discovery of a schema or manual
 * specification of a schema, through the {@link SimpleTableDef} class.
 *
 */
public class CassandraDataContext extends QueryPostprocessDataContext implements DataContext {

    private static final Logger logger = LoggerFactory.getLogger(CassandraDataContext.class);
    private final Cluster cassandraCluster;
    private final SimpleTableDef[] tableDefs;
    private final String keySpaceName;

    /**
     * Constructs a {@link CassandraDataContext}. This constructor accepts a
     * custom array of {@link SimpleTableDef}s which allows the user to define
     * his own view on the indexes in the engine.
     *
     * @param cluster
     *            the Cassandra cluster
     * @param keySpace
     *            the name of the Cassandra keyspace
     * @param tableDefs
     *            an array of {@link SimpleTableDef}s, which define the table
     *            and column model of the ElasticSearch index.
     */
    public CassandraDataContext(Cluster cluster, String keySpace, SimpleTableDef... tableDefs) {
        this.cassandraCluster = cluster;
        this.keySpaceName = keySpace;
        this.tableDefs = tableDefs;
    }

    /**
     * Constructs a {@link CassandraDataContext} and automatically detects the
     * schema structure/view on the keyspace (see
     * {@link #detectSchema(Cluster, String)}).
     *
     * @param cluster
     *            the Cassandra cluster
     * @param keySpace
     *            the name of the Cassandra keyspace to represent
     */
    public CassandraDataContext(Cluster cluster, String keySpace) {
        this(cluster, keySpace, detectSchema(cluster, keySpace));
    }

    /**
     * Performs an analysis of the given keyspace in a Cassandra cluster
     * {@link Cluster} instance and detects the cassandra types structure based
     * on the metadata provided by the datastax cassandra java client.
     *
     * @see #detectTable(TableMetadata)
     *
     * @param cluster
     *            the cluster to inspect
     * @param keyspaceName
     * @return a mutable schema instance, useful for further fine tuning by the
     *         user.
     */
    public static SimpleTableDef[] detectSchema(Cluster cluster, String keyspaceName) {
        final Metadata metadata = cluster.getMetadata();
        final KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
        if (keyspace == null) {
            throw new IllegalArgumentException("Keyspace '" + keyspaceName + "' does not exist in the database");
        }
        final Collection<TableMetadata> tables = keyspace.getTables();
        final SimpleTableDef[] result = new SimpleTableDef[tables.size()];
        int i = 0;
        for (final TableMetadata tableMetaData : tables) {
            final SimpleTableDef table = detectTable(tableMetaData);
            result[i] = table;
            i++;
        }
        return result;
    }

    /**
     * Performs an analysis of an available table in Cassandra.
     *
     * @param tableMetaData
     *            the table meta data
     * @return a table definition for cassandra.
     */
    public static SimpleTableDef detectTable(TableMetadata tableMetaData) {
        final List<ColumnMetadata> columns = tableMetaData.getColumns();
        final String[] columnNames = new String[columns.size()];
        final ColumnType[] columnTypes = new ColumnType[columns.size()];
        int i = 0;
        for (final ColumnMetadata column : columns) {
            columnNames[i] = column.getName();
            columnTypes[i] = getColumnTypeFromMetaDataField(column.getType().getName());
            i++;
        }

        return new SimpleTableDef(tableMetaData.getName(), columnNames, columnTypes);
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final MutableSchema theSchema = new MutableSchema(getMainSchemaName());
        for (final SimpleTableDef tableDef : tableDefs) {
            final MutableTable table = tableDef.toTable().setSchema(theSchema);

            final TableMetadata cassandraTable = cassandraCluster.getMetadata().getKeyspace(keySpaceName).getTable(table
                    .getName());
            if (cassandraTable != null) {
                final List<ColumnMetadata> primaryKeys = cassandraTable.getPrimaryKey();
                for (ColumnMetadata primaryKey : primaryKeys) {
                    final MutableColumn column = (MutableColumn) table.getColumnByName(primaryKey.getName());
                    if (column != null) {
                        column.setPrimaryKey(true);
                    }
                    column.setNativeType(primaryKey.getType().getName().name());
                }
            }

            theSchema.addTable(table);
        }
        return theSchema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return keySpaceName;
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        final Select query = QueryBuilder.select().all().from(keySpaceName, table.getName());
        if (limitMaxRowsIsSet(maxRows)) {
            query.limit(maxRows);
        }
        final ResultSet resultSet = cassandraCluster.connect().execute(query);

        final Iterator<Row> response = resultSet.iterator();
        return new CassandraDataSet(response, columns);
    }

    private boolean limitMaxRowsIsSet(int maxRows) {
        return (maxRows != -1);
    }

    @Override
    protected org.apache.metamodel.data.Row executePrimaryKeyLookupQuery(Table table, List<SelectItem> selectItems,
            Column primaryKeyColumn, Object keyValue) {
        
        if (primaryKeyColumn.getType() == ColumnType.UUID && keyValue instanceof String) {
            keyValue = UUID.fromString(keyValue.toString());
        }

        Selection select = QueryBuilder.select();
        for (SelectItem selectItem : selectItems) {
            final Column column = selectItem.getColumn();
            assert column != null;
            select = select.column(column.getName());
        }

        final Statement statement = select.from(keySpaceName, table.getName()).where(QueryBuilder.eq(primaryKeyColumn
                .getName(), keyValue));

        final Row row = cassandraCluster.connect().execute(statement).one();

        return CassandraUtils.toRow(row, new SimpleDataSetHeader(selectItems));
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        if (!whereItems.isEmpty()) {
            // not supported - will have to be done by counting client-side
            logger.debug(
                    "Not able to execute count query natively - resorting to query post-processing, which may be expensive");
            return null;
        }
        final Statement statement = QueryBuilder.select().countAll().from(keySpaceName, table.getName());
        final Row response = cassandraCluster.connect().execute(statement).one();
        return response.getLong(0);
    }

    private static ColumnType getColumnTypeFromMetaDataField(DataType.Name metaDataName) {
        switch (metaDataName) {
        case BIGINT:
        case COUNTER:
            return ColumnType.BIGINT;
        case BLOB:
            return ColumnType.BLOB;
        case BOOLEAN:
            return ColumnType.BOOLEAN;
        case DECIMAL:
            return ColumnType.DECIMAL;
        case DOUBLE:
            return ColumnType.DOUBLE;
        case FLOAT:
            return ColumnType.FLOAT;
        case INT:
            return ColumnType.INTEGER;
        case TEXT:
            return ColumnType.STRING;
        case TIMESTAMP:
            return ColumnType.TIMESTAMP;
        case UUID:
            return ColumnType.UUID;
        case VARCHAR:
            return ColumnType.VARCHAR;
        case VARINT:
            return ColumnType.BIGINT;
        case LIST:
            return ColumnType.LIST;
        case MAP:
            return ColumnType.MAP;
        case CUSTOM:
            return ColumnType.OTHER;
        case INET:
            return ColumnType.INET;
        case SET:
            return ColumnType.SET;
        default:
            return ColumnType.STRING;
        }
    }
}
