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
package org.apache.metamodel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.CompiledQuery;
import org.apache.metamodel.query.DefaultCompiledQuery;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.builder.InitFromBuilder;
import org.apache.metamodel.query.builder.InitFromBuilderImpl;
import org.apache.metamodel.query.parser.QueryParser;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

/**
 * Abstract implementation of the DataContext interface. Provides convenient
 * implementations of all trivial and datastore-independent methods.
 */
public abstract class AbstractDataContext implements DataContext {

    private static final String NULL_SCHEMA_NAME_TOKEN = "<metamodel.schema.name.null>";
    private final ConcurrentMap<String, Schema> _schemaCache = new ConcurrentHashMap<String, Schema>();
    private final Comparator<? super String> _schemaNameComparator = SchemaNameComparator.getInstance();
    private List<String> _schemaNameCache;

    /**
     * {@inheritDoc}
     */
    @Override
    public final DataContext refreshSchemas() {
        _schemaCache.clear();
        _schemaNameCache = null;
        onSchemaCacheRefreshed();
        return this;
    }

    /**
     * Method invoked when schemas have been refreshed using
     * {@link #refreshSchemas()}. Can be overridden to add callback
     * functionality in subclasses.
     */
    protected void onSchemaCacheRefreshed() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final List<Schema> getSchemas() throws MetaModelException {
        List<String> schemaNames = getSchemaNames();
        List<Schema> schemas = new ArrayList<>();
        for (final String name: schemaNames) {
            final Schema schema = _schemaCache.get(getSchemaCacheKey(name));
            if (schema == null) {
                final Schema newSchema = getSchemaByName(name);
                if (newSchema == null) {
                    throw new MetaModelException("Declared schema does not exist: " + name);
                }
                final Schema existingSchema = _schemaCache.putIfAbsent(getSchemaCacheKey(name), newSchema);
                if (existingSchema == null) {
                    schemas.add(newSchema);
                } else {
                    schemas.add(existingSchema);
                }
            } else {
                schemas.add(schema);
            }
        }
        return schemas;
    }

    private String getSchemaCacheKey(String name) {
        if (name == null) {
            return NULL_SCHEMA_NAME_TOKEN;
        }
        return name;
    }

    /**
     * m {@inheritDoc}
     */
    @Override
    public final List<String> getSchemaNames() throws MetaModelException {
        if (_schemaNameCache == null) {
            _schemaNameCache = getSchemaNamesInternal();
        }
        List<String> schemaNames = new ArrayList<>(_schemaNameCache);
        schemaNames.sort(_schemaNameComparator);
        return schemaNames;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Schema getDefaultSchema() throws MetaModelException {
        Schema result = null;
        final String defaultSchemaName = getDefaultSchemaName();
        if (defaultSchemaName != null) {
            result = getSchemaByName(defaultSchemaName);
        }
        if (result == null) {
            final List<Schema> schemas = getSchemas();
            if (schemas.size() == 1) {
                result = schemas.get(0);
            } else {
                int highestTableCount = -1;
                for (Schema schema: schemas) {
                    String name = schema.getName();
                    if (schema != null) {
                        name = name.toLowerCase();
                        final boolean isInformationSchema = name.startsWith("information") && name.endsWith("schema");
                        if (!isInformationSchema && schema.getTableCount() > highestTableCount) {
                            highestTableCount = schema.getTableCount();
                            result = schema;
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final InitFromBuilder query() {
        return new InitFromBuilderImpl(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query parseQuery(final String queryString) throws MetaModelException {
        final QueryParser parser = new QueryParser(this, queryString);
        final Query query = parser.parse();
        return query;
    }

    @Override
    public CompiledQuery compileQuery(final Query query) throws MetaModelException {
        return new DefaultCompiledQuery(query);
    }

    @Override
    public DataSet executeQuery(CompiledQuery compiledQuery, Object... values) {
        assert compiledQuery instanceof DefaultCompiledQuery;

        final DefaultCompiledQuery defaultCompiledQuery = (DefaultCompiledQuery) compiledQuery;
        final Query query = defaultCompiledQuery.cloneWithParameterValues(values);

        return executeQuery(query);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final DataSet executeQuery(final String queryString) throws MetaModelException {
        final Query query = parseQuery(queryString);
        final DataSet dataSet = executeQuery(query);
        return dataSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Schema getSchemaByName(String name) throws MetaModelException {
        Schema schema = _schemaCache.get(getSchemaCacheKey(name));
        if (schema == null) {
            if (name == null) {
                schema = getSchemaByNameInternal(null);
            } else {
                List<String> schemaNames = getSchemaNames();
                for (String schemaName : schemaNames) {
                    if (name.equalsIgnoreCase(schemaName)) {
                        schema = getSchemaByNameInternal(name);
                        break;
                    }
                }
                if (schema == null) {
                    for (String schemaName : schemaNames) {
                        if (name.equalsIgnoreCase(schemaName)) {
                            // try again with "schemaName" as param instead of
                            // "name".
                            schema = getSchemaByNameInternal(schemaName);
                            break;
                        }
                    }
                }
            }
            if (schema != null) {
                Schema existingSchema = _schemaCache.putIfAbsent(getSchemaCacheKey(schema.getName()), schema);
                if (existingSchema != null) {
                    // race conditions may cause two schemas to be created.
                    // We'll favor the existing schema if possible, since schema
                    // may contain lazy-loading logic and so on.
                    return existingSchema;
                }
            }
        }
        return schema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Column getColumnByQualifiedLabel(final String columnName) {
        if (columnName == null) {
            return null;
        }

        final String[] tokens = tokenizePath(columnName, 3);
        if (tokens != null) {
            final Schema schema = getSchemaByToken(tokens[0]);
            if (schema != null) {
                final Table table = schema.getTableByName(tokens[1]);
                if (table != null) {
                    final Column column = table.getColumnByName(tokens[2]);
                    if (column != null) {
                        return column;
                    }
                }
            }
        }

        Schema schema = null;
        final List<String> schemaNames = getSchemaNames();
        for (final String schemaName : schemaNames) {
            if (schemaName == null) {
                // search without schema name (some databases have only a single
                // schema with no name)
                schema = getSchemaByName(null);
                if (schema != null) {
                    Column column = getColumn(schema, columnName);
                    if (column != null) {
                        return column;
                    }
                }
            } else {
                // Search case-sensitive
                Column col = searchColumn(schemaName, columnName, columnName);
                if (col != null) {
                    return col;
                }
            }
        }

        final String columnNameInLowerCase = columnName.toLowerCase();
        for (final String schemaName : schemaNames) {
            if (schemaName != null) {
                // search case-insensitive
                String schameNameInLowerCase = schemaName.toLowerCase();
                Column col = searchColumn(schameNameInLowerCase, columnName, columnNameInLowerCase);
                if (col != null) {
                    return col;
                }
            }
        }

        schema = getDefaultSchema();
        if (schema != null) {
            Column column = getColumn(schema, columnName);
            if (column != null) {
                return column;
            }
        }

        return null;
    }

    /**
     * Searches for a particular column within a schema
     * 
     * @param schemaNameSearch
     *            the schema name to use for search
     * @param columnNameOriginal
     *            the original column name
     * @param columnNameSearch
     *            the column name as it should be searched for (either the same
     *            as original, or lower case in case of case-insensitive search)
     * @return
     */
    private Column searchColumn(String schemaNameSearch, String columnNameOriginal, String columnNameSearch) {
        if (columnNameSearch.startsWith(schemaNameSearch)) {
            Schema schema = getSchemaByName(schemaNameSearch);
            if (schema != null) {
                String tableAndColumnPath = columnNameOriginal.substring(schemaNameSearch.length());

                if (tableAndColumnPath.charAt(0) == '.') {
                    tableAndColumnPath = tableAndColumnPath.substring(1);

                    Column column = getColumn(schema, tableAndColumnPath);
                    if (column != null) {
                        return column;
                    }
                }
            }
        }
        return null;
    }

    private final Column getColumn(final Schema schema, final String tableAndColumnPath) {
        Table table = null;
        String columnPath = tableAndColumnPath;
        final List<String> tableNames = schema.getTableNames();
        for (final String tableName : tableNames) {
            if (tableName != null) {
                // search case-sensitive
                if (isStartingToken(tableName, tableAndColumnPath)) {
                    table = schema.getTableByName(tableName);
                    columnPath = tableAndColumnPath.substring(tableName.length());

                    if (columnPath.charAt(0) == '.') {
                        columnPath = columnPath.substring(1);
                        break;
                    }
                }
            }
        }

        if (table == null) {
            final String tableAndColumnPathInLowerCase = tableAndColumnPath.toLowerCase();
            for (final String tableName : tableNames) {
                if (tableName != null) {
                    String tableNameInLowerCase = tableName.toLowerCase();
                    // search case-insensitive
                    if (isStartingToken(tableNameInLowerCase, tableAndColumnPathInLowerCase)) {
                        table = schema.getTableByName(tableName);
                        columnPath = tableAndColumnPath.substring(tableName.length());

                        if (columnPath.charAt(0) == '.') {
                            columnPath = columnPath.substring(1);
                            break;
                        }
                    }
                }
            }
        }

        if (table == null && tableNames.size() == 1) {
            table = schema.getTables().get(0);
        }

        if (table != null) {
            Column column = table.getColumnByName(columnPath);
            if (column != null) {
                return column;
            }
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Table getTableByQualifiedLabel(final String tableName) {
        if (tableName == null) {
            return null;
        }

        final String[] tokens = tokenizePath(tableName, 2);
        if (tokens != null) {
            Schema schema = getSchemaByToken(tokens[0]);
            if (schema != null) {
                Table table = schema.getTableByName(tokens[1]);
                if (table != null) {
                    return table;
                }
            }
        }

        Schema schema = null;
        List<String> schemaNames = getSchemaNames();
        for (String schemaName : schemaNames) {
            if (schemaName == null) {
                // there's an unnamed schema present.
                schema = getSchemaByName(null);
                if (schema != null) {
                    Table table = schema.getTableByName(tableName);
                    return table;
                }
            } else {
                // case-sensitive search
                if (isStartingToken(schemaName, tableName)) {
                    schema = getSchemaByName(schemaName);
                }
            }
        }

        if (schema == null) {
            final String tableNameInLowerCase = tableName.toLowerCase();
            for (final String schemaName : schemaNames) {
                if (schemaName != null) {
                    // case-insensitive search
                    final String schemaNameInLowerCase = schemaName.toLowerCase();
                    if (isStartingToken(schemaNameInLowerCase, tableNameInLowerCase)) {
                        schema = getSchemaByName(schemaName);
                    }
                }
            }
        }

        if (schema == null) {
            schema = getDefaultSchema();
        }

        String tablePart = tableName.toLowerCase();
        String schemaName = schema.getName();
        if (schemaName != null && isStartingToken(schemaName.toLowerCase(), tablePart)) {
            tablePart = tablePart.substring(schemaName.length());
            if (tablePart.startsWith(".")) {
                tablePart = tablePart.substring(1);
            }
        }

        return schema.getTableByName(tablePart);
    }

    /**
     * Tokenizes a path for a table or a column.
     * 
     * @param path
     * @param expectedParts
     * @return
     */
    private String[] tokenizePath(String path, int expectedParts) {
        final List<String> tokens = new ArrayList<String>(expectedParts);

        boolean inQuotes = false;
        final StringBuilder currentToken = new StringBuilder();
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '.' && !inQuotes) {
                // token finished
                tokens.add(currentToken.toString());
                currentToken.setLength(0);

                if (tokens.size() > expectedParts) {
                    // unsuccesfull - return null
                    return null;
                }
            } else if (c == '"') {
                if (inQuotes) {
                    if (i + 1 < path.length() && path.charAt(i + 1) != '.') {
                        // unsuccesfull - return null
                        return null;
                    }
                } else {
                    if (currentToken.length() > 0) {
                        // unsuccesfull - return null
                        return null;
                    }
                }
                inQuotes = !inQuotes;
            } else {
                currentToken.append(c);
            }
        }

        if (currentToken.length() > 0) {
            tokens.add(currentToken.toString());
        }

        if (tokens.size() == expectedParts - 1) {
            // add a special-meaning "null" which will be interpreted as the
            // default schema (since the schema wasn't specified).
            tokens.add(0, null);
        } else if (tokens.size() != expectedParts) {
            return null;
        }

        return tokens.toArray(new String[tokens.size()]);
    }

    private Schema getSchemaByToken(String token) {
        if (token == null) {
            return getDefaultSchema();
        }
        try {
            return getSchemaByName(token);
        } catch (RuntimeException e) {
            // swallow this exception - the attempt did not work and the null
            // will be treated.
            return null;
        }
    }

    private boolean isStartingToken(String partName, String fullName) {
        if (fullName.startsWith(partName)) {
            final int length = partName.length();
            if (length == 0) {
                return false;
            }
            if (fullName.length() > length) {
                final char nextChar = fullName.charAt(length);
                if (isQualifiedPathDelim(nextChar)) {
                    return true;
                }
            }
        }
        return false;
    }

    protected boolean isQualifiedPathDelim(char c) {
        return c == '.' || c == '"';
    }

    /**
     * Gets schema names from the non-abstract implementation. These schema
     * names will be cached except if the {@link #refreshSchemas()} method is
     * called.
     * 
     * @return an array of schema names.
     */
    protected abstract List<String> getSchemaNamesInternal();

    /**
     * Gets the name of the default schema.
     * 
     * @return the default schema name.
     */
    protected abstract String getDefaultSchemaName();

    /**
     * Gets a specific schema from the non-abstract implementation. This schema
     * object will be cached except if the {@link #refreshSchemas()} method is
     * called.
     * 
     * @param name
     *            the name of the schema to get
     * @return a schema object representing the named schema, or null if no such
     *         schema exists.
     */
    protected abstract Schema getSchemaByNameInternal(String name);
}