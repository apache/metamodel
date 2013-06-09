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
package org.eobjects.metamodel.salesforce;

import java.util.ArrayList;
import java.util.List;

import org.eobjects.metamodel.schema.AbstractSchema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.LazyRef;
import org.eobjects.metamodel.util.Ref;

import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

/**
 * Schema implementation for Salesforce, which lazy loads tables based on the
 * "describe" web services.
 */
final class SalesforceSchema extends AbstractSchema {

    private static final long serialVersionUID = 1L;

    private final String _name;
    private final transient Ref<List<Table>> _tableRef;
    private final transient PartnerConnection _connection;

    public SalesforceSchema(String name, PartnerConnection connection) {
        _name = name;
        _connection = connection;
        _tableRef = new LazyRef<List<Table>>() {
            @Override
            protected List<Table> fetch() {
                final List<Table> result = new ArrayList<Table>();
                final DescribeGlobalResult describeGlobal;
                try {
                    describeGlobal = _connection.describeGlobal();
                } catch (ConnectionException e) {
                    throw SalesforceUtils.wrapException(e, "Failed to invoke describeGlobal service");
                }

                for (final DescribeGlobalSObjectResult sobject : describeGlobal.getSobjects()) {
                    if (sobject.isQueryable() && sobject.isUpdateable()) {
                        final String tableName = sobject.getName();
                        final String tableLabel = sobject.getLabel();

                        final Table table = new SalesforceTable(tableName, tableLabel, SalesforceSchema.this,
                                _connection);
                        result.add(table);
                    }
                }
                return result;
            }
        };
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Table[] getTables() {
        List<Table> tables = _tableRef.get();
        return tables.toArray(new Table[tables.size()]);
    }

    @Override
    public String getQuote() {
        return null;
    }

}
