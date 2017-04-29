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
package org.apache.metamodel.sugarcrm;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.metamodel.schema.AbstractSchema;
import org.apache.metamodel.schema.Table;

import com.sugarcrm.ws.soap.SugarsoapPortType;

final class SugarCrmSchema extends AbstractSchema {

    private static final long serialVersionUID = 1L;

    private final List<Table> _tables;
    private final String _name;

    public SugarCrmSchema(String name, SugarsoapPortType service, Supplier<String> sessionId) {
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
