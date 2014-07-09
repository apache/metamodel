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
package org.apache.metamodel.schema.builder;

import java.util.Map;

import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.ResourceUtils;

/**
 * {@link InferentialSchemaBuilder} that produces a single table.
 */
public class SingleTableInferentialSchemaBuilder extends InferentialSchemaBuilder {

    private final String _tableName;

    public SingleTableInferentialSchemaBuilder(Resource resource) {
        this(ResourceUtils.getParentName(resource), resource.getName());
    }

    public SingleTableInferentialSchemaBuilder(String schemaName, String tableName) {
        super(schemaName);
        _tableName = tableName;
    }

    @Override
    public void offerSource(DocumentSource documentSource) {
        getTableBuilder(_tableName).offerSource(documentSource);
    }

    @Override
    protected String determineTable(Map<String, ?> map) {
        return _tableName;
    }

    @Override
    public DocumentConverter getDocumentConverter(Table table) {
        return new ColumnNameAsKeysRowConverter();
    }

}
