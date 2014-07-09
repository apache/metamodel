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
package org.apache.metamodel.couchdb;

import org.apache.metamodel.convert.DocumentConverter;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.builder.DocumentSourceProvider;
import org.apache.metamodel.schema.builder.SimpleTableDefSchemaBuilder;
import org.apache.metamodel.util.SimpleTableDef;

public class CouchDbSimpleTableDefSchemaBuilder extends SimpleTableDefSchemaBuilder {
    
    public CouchDbSimpleTableDefSchemaBuilder(SimpleTableDef[] tableDefs) {
        super(CouchDbDataContext.SCHEMA_NAME, tableDefs);
    }

    @Override
    public void offerSources(DocumentSourceProvider documentSourceProvider) {

    }

    @Override
    public MutableSchema build() {
        MutableSchema schema = super.build();
        MutableTable[] tables = schema.getTables();
        for (MutableTable table : tables) {
            CouchDbTableCreationBuilder.addMandatoryColumns(table);
        }
        return schema;
    }

    @Override
    public DocumentConverter getDocumentConverter(Table table) {
        return new CouchDbDocumentConverter();
    }

}
