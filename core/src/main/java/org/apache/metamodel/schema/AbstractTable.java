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
package org.apache.metamodel.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.util.CollectionUtils;
import org.apache.metamodel.util.HasNameMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract {@link Table} implementation. Includes most common/trivial methods.
 */
public abstract class AbstractTable implements Table {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(AbstractTable.class);

    @Override
    public final int getColumnCount() {
        return getColumns().length;
    }

    @Override
    public Column getColumn(int index) throws IndexOutOfBoundsException {
        Column[] columns = getColumns();
        return columns[index];
    }

    @Override
    public final Column getColumnByName(final String columnName) {
        if (columnName == null) {
            return null;
        }

        final List<Column> foundColumns = new ArrayList<Column>(1);

        // Search for column matches, case insensitive.
        for (Column column : getColumns()) {
            final String candidateName = column.getName();
            if (columnName.equalsIgnoreCase(candidateName)) {
                foundColumns.add(column);
            }
        }

        final int numColumns = foundColumns.size();

        if (logger.isDebugEnabled()) {
            logger.debug("Found {} column(s) matching '{}': {}", new Object[] { numColumns, columnName, foundColumns });
        }

        if (numColumns == 0) {
            return null;
        } else if (numColumns == 1) {
            // if there's only one, return it.
            return foundColumns.get(0);
        }

        // If more matches are found, search case sensitive
        for (Column column : foundColumns) {
            if (columnName.equals(column.getName())) {
                return column;
            }
        }

        // if none matches case sensitive, pick the first one.
        return foundColumns.get(0);
    }

    @Override
    public final int getRelationshipCount() {
        return getRelationships().length;
    }

    @Override
    public final Column[] getNumberColumns() {
        return CollectionUtils.filter(getColumns(), new Predicate<Column>() {
            @Override
            public boolean test(Column col) {
                ColumnType type = col.getType();
                return type != null && type.isNumber();
            }
        }).toArray(new Column[0]);
    }

    @Override
    public final Column[] getLiteralColumns() {
        return CollectionUtils.filter(getColumns(), new Predicate<Column>() {
            @Override
            public boolean test(Column col) {
                ColumnType type = col.getType();
                return type != null && type.isLiteral();
            }
        }).toArray(new Column[0]);
    }

    @Override
    public final Column[] getTimeBasedColumns() {
        return CollectionUtils.filter(getColumns(), col -> {
            final ColumnType type = col.getType();
            return type != null && type.isTimeBased();
        }).toArray(new Column[0]);
    }

    @Override
    public final Column[] getBooleanColumns() {
        return CollectionUtils.filter(getColumns(), col -> {
            final ColumnType type = col.getType();
            return type != null && type.isBoolean();
        }).toArray(new Column[0]);
    }

    @Override
    public final Column[] getIndexedColumns() {
        return CollectionUtils.filter(getColumns(), Column::isIndexed).toArray(new Column[0]);
    }

    @Override
    public final Relationship[] getForeignKeyRelationships() {
        return CollectionUtils.filter(getRelationships(), rel -> {
            return AbstractTable.this.equals(rel.getForeignTable());
        }).toArray(new Relationship[0]);
    }

    @Override
    public final Relationship[] getPrimaryKeyRelationships() {
        return CollectionUtils.filter(getRelationships(), rel -> {
            return AbstractTable.this.equals(rel.getPrimaryTable());
        }).toArray(new Relationship[0]);
    }

    @Override
    public final Column[] getForeignKeys() {
        final Set<Column> columns = new HashSet<Column>();
        final Relationship[] relationships = getForeignKeyRelationships();
        CollectionUtils.forEach(relationships, rel -> {
            Column[] foreignColumns = rel.getForeignColumns();
            for (Column column : foreignColumns) {
                columns.add(column);
            }
        });
        return columns.toArray(new Column[columns.size()]);
    }

    @Override
    public final Column[] getPrimaryKeys() {
        final List<Column> primaryKeyColumns = new ArrayList<Column>();
        final Column[] columnsInTable = getColumns();
        for (Column column : columnsInTable) {
            if (column.isPrimaryKey()) {
                primaryKeyColumns.add(column);
            }
        }
        return primaryKeyColumns.toArray(new Column[primaryKeyColumns.size()]);
    }

    @Override
    public final String[] getColumnNames() {
        Column[] columns = getColumns();
        return CollectionUtils.map(columns, new HasNameMapper()).toArray(new String[columns.length]);
    }

    @Override
    public final Column[] getColumnsOfType(ColumnType columnType) {
        Column[] columns = getColumns();
        return MetaModelHelper.getColumnsByType(columns, columnType);
    }

    @Override
    public final Column[] getColumnsOfSuperType(final SuperColumnType superColumnType) {
        Column[] columns = getColumns();
        return MetaModelHelper.getColumnsBySuperType(columns, superColumnType);
    }

    @Override
    public final Relationship[] getRelationships(final Table otherTable) {
        Relationship[] relationships = getRelationships();

        return CollectionUtils.filter(relationships, relation -> {
            if (relation.getForeignTable() == otherTable && relation.getPrimaryTable() == AbstractTable.this) {
                return true;
            } else if (relation.getForeignTable() == AbstractTable.this && relation.getPrimaryTable() == otherTable) {
                return true;
            }
            return false;
        }).toArray(new Relationship[0]);
    }

    @Override
    public final String getQuotedName() {
        String quote = getQuote();
        if (quote == null) {
            return getName();
        }
        return quote + getName() + quote;
    }

    @Override
    public final String getQualifiedLabel() {
        StringBuilder sb = new StringBuilder();
        Schema schema = getSchema();
        if (schema != null && schema.getName() != null) {
            sb.append(schema.getQualifiedLabel());
            sb.append('.');
        }
        sb.append(getName());
        return sb.toString();
    }

    @Override
    public final String toString() {
        return "Table[name=" + getName() + ",type=" + getType() + ",remarks=" + getRemarks() + "]";
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof Table) {
            final Table other = (Table) obj;
            if (!getQualifiedLabel().equals(other.getQualifiedLabel())) {
                return false;
            }
            if (getType() != other.getType()) {
                return false;
            }
            final Schema sch1 = getSchema();
            final Schema sch2 = other.getSchema();
            if (sch1 != null) {
                if (!sch1.equals(sch2)) {
                    return false;
                }
            } else {
                if (sch2 != null) {
                    return false;
                }
            }

            try {
                final String[] columnNames1 = getColumnNames();
                final String[] columnNames2 = other.getColumnNames();

                if (columnNames1 != null && columnNames1.length != 0) {
                    if (columnNames2 != null && columnNames2.length != 0) {
                        if (!Arrays.equals(columnNames1, columnNames2)) {
                            return false;
                        }
                    }
                }
            } catch (Exception e) {
                // going "down stream" may throw exceptions, e.g. due to
                // de-serialization issues. We will be tolerant to such
                // exceptions
                logger.debug("Caught (and ignoring) exception while comparing column names of tables", e);
            }

            return true;
        }
        return false;
    }

    @Override
    public final int compareTo(Table that) {
        int diff = getQualifiedLabel().compareTo(that.getQualifiedLabel());
        if (diff == 0) {
            diff = toString().compareTo(that.toString());
        }
        return diff;
    }
}
