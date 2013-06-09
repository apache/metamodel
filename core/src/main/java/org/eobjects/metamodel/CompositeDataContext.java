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
package org.eobjects.metamodel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.CompositeSchema;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.Func;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataContext for composite datacontexts. Composite DataContexts wrap several
 * other datacontexts and makes cross-datastore querying possible.
 * 
 * @author Kasper Sørensen
 */
public class CompositeDataContext extends AbstractDataContext {

    private final static Logger logger = LoggerFactory.getLogger(CompositeDataContext.class);
    private Map<String, CompositeSchema> _compositeSchemas = new HashMap<String, CompositeSchema>();
    private DataContext[] _delegates;

    public CompositeDataContext(DataContext... delegates) {
        if (delegates == null) {
            throw new IllegalArgumentException("delegates cannot be null");
        }
        _delegates = delegates;
    }

    public CompositeDataContext(Collection<DataContext> delegates) {
        if (delegates == null) {
            throw new IllegalArgumentException("delegates cannot be null");
        }
        _delegates = delegates.toArray(new DataContext[delegates.size()]);
    }

    @Override
    public DataSet executeQuery(Query query) throws MetaModelException {
        // a set of all datacontexts involved
        Set<DataContext> dataContexts = new HashSet<DataContext>();

        // find all datacontexts involved, by investigating FROM items
        List<FromItem> items = query.getFromClause().getItems();
        for (FromItem item : items) {
            List<FromItem> tableFromItems = MetaModelHelper.getTableFromItems(item);
            for (FromItem fromItem : tableFromItems) {
                Table table = fromItem.getTable();

                DataContext dc = getDataContext(table);
                if (dc == null) {
                    throw new MetaModelException("Could not resolve child-datacontext for table: " + table);
                }
                dataContexts.add(dc);
            }
        }

        if (dataContexts.isEmpty()) {
            throw new MetaModelException("No suiting delegate DataContext to execute query: " + query);
        } else if (dataContexts.size() == 1) {
            Iterator<DataContext> it = dataContexts.iterator();
            assert it.hasNext();
            DataContext dc = it.next();
            return dc.executeQuery(query);
        } else {
            // we create a datacontext which can materialize tables from
            // separate datacontexts.
            final Func<Table, DataContext> dataContextRetrievalFunction = new Func<Table, DataContext>() {
                @Override
                public DataContext eval(Table table) {
                    return getDataContext(table);
                }
            };
            return new CompositeQueryDelegate(dataContextRetrievalFunction).executeQuery(query);
        }
    }

    private DataContext getDataContext(Table table) {
        DataContext result = null;
        if (table != null) {
            Schema schema = table.getSchema();

            if (schema != null) {
                for (DataContext dc : _delegates) {
                    Schema dcSchema = dc.getSchemaByName(schema.getName());
                    if (dcSchema != null) {

                        // first round = try with schema identity match
                        if (dcSchema == schema) {
                            logger.debug("DataContext for '{}' resolved (using identity) to: '{}'", table, dcSchema);
                            result = dc;
                            break;
                        }
                    }
                }

                if (result == null) {
                    for (DataContext dc : _delegates) {
                        Schema dcSchema = dc.getSchemaByName(schema.getName());
                        if (dcSchema != null) {
                            // second round = try with schema equals method
                            if (dcSchema.equals(schema)) {
                                logger.debug("DataContext for '{}' resolved (using equals) to: '{}'", table, dcSchema);
                                result = dc;
                                break;
                            }
                        }
                    }
                }
            }
        }

        if (result == null) {
            logger.warn("Couldn't resolve DataContext for {}", table);
        }
        return result;
    }

    @Override
    public String getDefaultSchemaName() throws MetaModelException {
        for (DataContext dc : _delegates) {
            Schema schema = dc.getDefaultSchema();
            if (schema != null) {
                return schema.getName();
            }
        }
        return null;
    }

    @Override
    public Schema getSchemaByNameInternal(String name) throws MetaModelException {
        CompositeSchema compositeSchema = _compositeSchemas.get(name);
        if (compositeSchema != null) {
            return compositeSchema;
        }
        List<Schema> matchingSchemas = new ArrayList<Schema>();
        for (DataContext dc : _delegates) {
            Schema schema = dc.getSchemaByName(name);
            if (schema != null) {
                matchingSchemas.add(schema);
            }
        }
        if (matchingSchemas.size() == 1) {
            return matchingSchemas.iterator().next();
        }
        if (matchingSchemas.size() > 1) {
            if (logger.isInfoEnabled()) {
                logger.info("Name-clash detected for Schema '{}'. Creating CompositeSchema.");
            }
            compositeSchema = new CompositeSchema(name, matchingSchemas);
            _compositeSchemas.put(name, compositeSchema);
            return compositeSchema;
        }
        return null;
    }

    @Override
    public String[] getSchemaNamesInternal() throws MetaModelException {
        Set<String> set = new HashSet<String>();
        for (DataContext dc : _delegates) {
            String[] schemaNames = dc.getSchemaNames();
            for (String name : schemaNames) {
                if (!MetaModelHelper.isInformationSchema(name)) {
                    // we skip information schemas, since they're anyways going
                    // to be incomplete and misleading.
                    set.add(name);
                }
            }
        }
        String[] result = set.toArray(new String[set.size()]);
        Arrays.sort(result);
        return result;
    }

}