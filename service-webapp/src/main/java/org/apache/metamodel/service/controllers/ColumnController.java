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
package org.apache.metamodel.service.controllers;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.service.app.DataContextTraverser;
import org.apache.metamodel.service.app.TenantContext;
import org.apache.metamodel.service.app.TenantRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = { "/{tenant}/{dataContext}/schemas/{schema}/tables/{table}/columns/{column}",
        "/{tenant}/{dataContext}/s/{schema}/t/{table}/c/{column}" }, produces = MediaType.APPLICATION_JSON_VALUE)
public class ColumnController {

    private final TenantRegistry tenantRegistry;

    @Autowired
    public ColumnController(TenantRegistry tenantRegistry) {
        this.tenantRegistry = tenantRegistry;
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> get(@PathVariable("tenant") String tenantId,
            @PathVariable("dataContext") String dataSourceName, @PathVariable("schema") String schemaId,
            @PathVariable("table") String tableId, @PathVariable("column") String columnId) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);
        final DataContext dataContext = tenantContext.getDataSourceRegistry().openDataContext(dataSourceName);

        final DataContextTraverser traverser = new DataContextTraverser(dataContext);

        final Column column = traverser.getColumn(schemaId, tableId, columnId);

        final String tenantName = tenantContext.getTenantName();
        final String tableName = column.getTable().getName();
        final String schemaName = column.getTable().getSchema().getName();

        final Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("number", column.getColumnNumber());
        metadata.put("size", column.getColumnSize());
        metadata.put("nullable", column.isNullable());
        metadata.put("primary-key", column.isPrimaryKey());
        metadata.put("indexed", column.isIndexed());
        metadata.put("column-type", column.getType() == null ? null : column.getType().getName());
        metadata.put("native-type", column.getNativeType());
        metadata.put("remarks", column.getRemarks());

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "column");
        map.put("name", column.getName());
        map.put("table", tableName);
        map.put("schema", schemaName);
        map.put("data-context", dataSourceName);
        map.put("tenant", tenantName);
        map.put("metadata", metadata);

        return map;
    }
}
