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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A composite schema, comprising tables from several {@link DataContext}s.
 * 
 * @author Kasper Sørensen
 */
public class CompositeSchema extends AbstractSchema {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(CompositeSchema.class);

    private final String name;
    private final Collection<? extends Schema> delegates;

    public CompositeSchema(String name, Collection<? extends Schema> delegates) {
        super();
        this.name = name;
        this.delegates = delegates;
        if (logger.isWarnEnabled()) {
            Set<String> names = new HashSet<String>();
            for (Table table : getTables()) {
                if (names.contains(table.getName())) {
                    logger.warn("Name-clash detected for Table {}.", table.getName());
                    logger.warn("getTableByName(\"{}\") will return just the first table.", table.getName());
                } else {
                    names.add(table.getName());
                }
            }
            if (!names.isEmpty()) {
                logger.warn("The following table names clashes in composite schema: " + names);
            }
        }
    }

    @Override
    public Relationship[] getRelationships() {
        Relationship[] result = new Relationship[0];
        for (Schema delegate : delegates) {
            result = CollectionUtils.array(result, delegate.getRelationships());
        }
        return result;
    }

    @Override
    public Table[] getTables() {
        Table[] result = new Table[0];
        for (Schema delegate : delegates) {
            result = CollectionUtils.array(result, delegate.getTables());
        }
        return result;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getQuote() {
        return null;
    }
}