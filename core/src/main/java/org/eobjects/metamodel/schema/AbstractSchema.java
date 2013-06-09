/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.schema;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.eobjects.metamodel.util.Action;
import org.eobjects.metamodel.util.CollectionUtils;
import org.eobjects.metamodel.util.EqualsBuilder;
import org.eobjects.metamodel.util.HasNameMapper;
import org.eobjects.metamodel.util.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of the {@link Schema} interface. Implements most
 * common and trivial methods.
 * 
 * @author Kasper Sørensen
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
        CollectionUtils.forEach(getTables(), new Action<Table>() {
            @Override
            public void run(Table table) {
                Relationship[] relations = table.getRelationships();
                for (int i = 0; i < relations.length; i++) {
                    Relationship relation = relations[i];
                    result.add(relation);
                }
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
        return CollectionUtils.filter(getTables(), new Predicate<Table>() {
            @Override
            public Boolean eval(Table table) {
                return table.getType() == type;
            }
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
                    // might occur when schemas are disconnected. Omit this check then.
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