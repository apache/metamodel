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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.metamodel.util.CollectionUtils;
import org.apache.metamodel.util.EqualsBuilder;
import org.apache.metamodel.util.HasNameMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of the {@link Schema} interface. Implements most
 * common and trivial methods.
 */
public abstract class AbstractSchema implements Schema {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(AbstractSchema.class);

    @Override
    public final String getQuotedName() {
        String quote = getQuote();
        if (quote == null) {
            return getName();
        }
        return quote + getName() + quote;
    }

    @Override
    public Relationship[] getRelationships() {
        final Set<Relationship> result = new LinkedHashSet<Relationship>();
        CollectionUtils.forEach(getTables(), table -> {
            final Relationship[] relations = table.getRelationships();
            for (int i = 0; i < relations.length; i++) {
                final Relationship relation = relations[i];
                result.add(relation);
            }
        });
        return result.toArray(new Relationship[result.size()]);
    }

    @Override
    public Table getTable(int index) throws IndexOutOfBoundsException {
        Table[] tables = getTables();
        return tables[index];
    }

    @Override
    public final String getQualifiedLabel() {
        return getName();
    }

    @Override
    public final int getTableCount(TableType type) {
        return getTables(type).length;
    }

    @Override
    public final int getRelationshipCount() {
        return getRelationships().length;
    }

    @Override
    public final int getTableCount() {
        return getTables().length;
    }

    @Override
    public final Table[] getTables(final TableType type) {
        return CollectionUtils.filter(getTables(), table -> {
            return table.getType() == type;
        }).toArray(new Table[0]);
    }

    @Override
    public final Table getTableByName(String tableName) {
        if (tableName == null) {
            return null;
        }

        final List<Table> foundTables = new ArrayList<Table>(1);
        // Search for table matches, case insensitive.
        for (Table table : getTables()) {
            if (tableName.equalsIgnoreCase(table.getName())) {
                foundTables.add(table);
            }
        }

        final int numTables = foundTables.size();
        if (logger.isDebugEnabled()) {
            logger.debug("Found {} tables(s) matching '{}': {}", new Object[] { numTables, tableName, foundTables });
        }

        if (numTables == 0) {
            return null;
        } else if (numTables == 1) {
            return foundTables.get(0);
        }

        // If more matches are found, search case sensitive
        for (Table table : foundTables) {
            if (tableName.equals(table.getName())) {
                return table;
            }
        }

        // if none matches case sensitive, pick the first one.
        return foundTables.get(0);
    }

    @Override
    public final String[] getTableNames() {
        Table[] tables = getTables();
        return CollectionUtils.map(tables, new HasNameMapper()).toArray(new String[tables.length]);
    }

    @Override
    public final String toString() {
        return "Schema[name=" + getName() + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof Schema) {
            Schema other = (Schema) obj;
            EqualsBuilder eb = new EqualsBuilder();
            eb.append(getName(), other.getName());
            eb.append(getQuote(), other.getQuote());
            if (eb.isEquals()) {
                try {
                    int tableCount1 = getTableCount();
                    int tableCount2 = other.getTableCount();
                    eb.append(tableCount1, tableCount2);
                } catch (Exception e) {
                    // might occur when schemas are disconnected. Omit this
                    // check then.
                }
            }
            return eb.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        String name = getName();
        if (name == null) {
            return -1;
        }
        return name.hashCode();
    }

    @Override
    public final int compareTo(Schema that) {
        int diff = getQualifiedLabel().compareTo(that.getQualifiedLabel());
        if (diff == 0) {
            diff = toString().compareTo(that.toString());
        }
        return diff;
    }
}