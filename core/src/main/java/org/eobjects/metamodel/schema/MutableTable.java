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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Represents the metadata about a table. Tables reside within a schema and
 * contains columns and relationships to other tables.
 * 
 * @see Schema
 * @see Column
 * @see Relationship
 */
public class MutableTable extends AbstractTable implements Serializable {

    private static final long serialVersionUID = -5094888465714027354L;
    protected String _name;
    protected TableType _type;
    protected String _remarks;
    protected Schema _schema;
    protected final List<Column> _columns;
    protected final List<Relationship> _relationships;
    protected String _quoteString = null;

    public MutableTable() {
        super();
        _columns = new ArrayList<Column>();
        _relationships = new ArrayList<Relationship>();
    }

    public MutableTable(String name) {
        this();
        setName(name);
    }

    public MutableTable(String name, TableType type) {
        this(name);
        _type = type;
    }

    public MutableTable(String name, TableType type, Schema schema) {
        this(name, type);
        _schema = schema;
    }

    public MutableTable(String name, TableType type, Schema schema, Column... columns) {
        this(name, type, schema);
        setColumns(columns);
    }

    @Override
    public String getName() {
        return _name;
    }

    public MutableTable setName(String name) {
        _name = name;
        return this;
    }

    /**
     * Internal getter for the columns of the table. Overwrite this method to
     * implement column lookup, column lazy-loading or similar.
     */
    protected List<Column> getColumnsInternal() {
        return _columns;
    }

    /**
     * Internal getter for the relationships of the table. Overwrite this method
     * to implement relationship lookup, relationship lazy-loading or similar.
     */
    protected List<Relationship> getRelationshipsInternal() {
        return _relationships;
    }

    @Override
    public Column[] getColumns() {
        List<Column> columns = getColumnsInternal();
        return columns.toArray(new Column[columns.size()]);
    }

    public MutableTable setColumns(Column... columns) {
        _columns.clear();
        for (Column column : columns) {
            _columns.add(column);
        }
        return this;
    }

    public MutableTable setColumns(Collection<Column> columns) {
        _columns.clear();
        for (Column column : columns) {
            _columns.add(column);
        }
        return this;
    }

    public MutableTable addColumn(Column column) {
        _columns.add(column);
        return this;
    }

    public MutableTable addColumn(int index, Column column) {
        _columns.add(index, column);
        return this;
    }

    public MutableTable removeColumn(Column column) {
        _columns.remove(column);
        return this;
    }

    @Override
    public Schema getSchema() {
        return _schema;
    }

    public MutableTable setSchema(Schema schema) {
        _schema = schema;
        return this;
    }

    @Override
    public TableType getType() {
        return _type;
    }

    public MutableTable setType(TableType type) {
        _type = type;
        return this;
    }

    @Override
    public Relationship[] getRelationships() {
        List<Relationship> relationships = getRelationshipsInternal();
        return relationships.toArray(new Relationship[relationships.size()]);
    }

    /**
     * Protected method for adding a relationship to this table. Should not be
     * used. Use Relationship.createRelationship(Column[], Column[]) instead.
     */
    protected void addRelationship(Relationship relation) {
        for (Relationship existingRelationship : _relationships) {
            if (existingRelationship.equals(relation)) {
                // avoid duplicate relationships
                return;
            }
        }
        _relationships.add(relation);
    }

    /**
     * Protected method for removing a relationship from this table. Should not
     * be used. Use Relationship.remove() instead.
     */
    protected MutableTable removeRelationship(Relationship relation) {
        _relationships.remove(relation);
        return this;
    }

    @Override
    public String getRemarks() {
        return _remarks;
    }

    public MutableTable setRemarks(String remarks) {
        _remarks = remarks;
        return this;
    }

    @Override
    public String getQuote() {
        return _quoteString;
    }

    public MutableTable setQuote(String quoteString) {
        _quoteString = quoteString;
        return this;
    }
}