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
package org.eobjects.metamodel.sugarcrm;

import java.util.ArrayList;
import java.util.List;

import org.eobjects.metamodel.schema.AbstractSchema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.Ref;

import com.sugarcrm.ws.soap.SugarsoapPortType;

final class SugarCrmSchema extends AbstractSchema {

    private static final long serialVersionUID = 1L;

    private final List<Table> _tables;
    private final String _name;

    public SugarCrmSchema(String name, SugarsoapPortType service, Ref<String> sessionId) {
        _name = name;
        _tables = new ArrayList<Table>();
        _tables.add(new SugarCrmTable("Accounts", this, service, sessionId));
        _tables.add(new SugarCrmTable("Employees", this, service, sessionId));
        _tables.add(new SugarCrmTable("Contacts", this, service, sessionId));
        _tables.add(new SugarCrmTable("Opportunities", this, service, sessionId));
        _tables.add(new SugarCrmTable("Leads", this, service, sessionId));
        _tables.add(new SugarCrmTable("Campaigns", this, service, sessionId));
        _tables.add(new SugarCrmTable("Emails", this, service, sessionId));
        _tables.add(new SugarCrmTable("Calls", this, service, sessionId));
        _tables.add(new SugarCrmTable("Meetings", this, service, sessionId));
        _tables.add(new SugarCrmTable("Tasks", this, service, sessionId));
        _tables.add(new SugarCrmTable("Notes", this, service, sessionId));
        _tables.add(new SugarCrmTable("Cases", this, service, sessionId));
        _tables.add(new SugarCrmTable("Prospects", this, service, sessionId));
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Table[] getTables() {
        return _tables.toArray(new Table[_tables.size()]);
    }

    @Override
    public String getQuote() {
        return null;
    }
}
