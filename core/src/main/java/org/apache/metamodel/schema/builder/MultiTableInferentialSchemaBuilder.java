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
 * {@link InferentialSchemaBuilder} that produces multiple table .
 */
public class MultiTableInferentialSchemaBuilder extends InferentialSchemaBuilder {

    private final String _discriminatorColumn;

    public MultiTableInferentialSchemaBuilder(Resource resource, String discriminatorColumn) {
        this(ResourceUtils.getParentName(resource), discriminatorColumn);
    }

    public MultiTableInferentialSchemaBuilder(String schemaName, String discriminatorColumn) {
        super(schemaName);
        _discriminatorColumn = discriminatorColumn;
    }


    @Override
    protected String determineTable(Map<String, ?> map) {
        Object value = map.get(_discriminatorColumn);
        if (value == null) {
            return "(other)";
        }
        return value.toString();
    }

    @Override
    public DocumentConverter getDocumentConverter(Table table) {
        return new ColumnNameAsKeysRowConverter();
    }

}
