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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.core.UriBuilder;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.service.app.TenantContext;
import org.apache.metamodel.service.app.TenantRegistry;
import org.apache.metamodel.service.controllers.model.RestLink;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/{tenant}/{dataContext}/schemas/{schema}/tables/{table}", produces = MediaType.APPLICATION_JSON_VALUE)
public class TableController {

    private final TenantRegistry tenantRegistry;

    @Autowired
    public TableController(TenantRegistry tenantRegistry) {
        this.tenantRegistry = tenantRegistry;
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> get(@PathVariable("tenant") String tenantId,
            @PathVariable("dataContext") String dataContextId, @PathVariable("schema") String schemaId,
            @PathVariable("table") String tableId) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);
        final DataContext dataContext = tenantContext.getDataContextRegistry().openDataContext(dataContextId);

        final Schema schema = dataContext.getSchemaByName(schemaId);
        final Table table = schema.getTableByName(tableId);

        final String tenantIdentifier = tenantContext.getTenantIdentifier();
        final UriBuilder uriBuilder = UriBuilder.fromPath(
                "/{tenant}/{dataContext}/schemas/{schema}/tables/{table}/columns/{column}");

        final String tableName = table.getName();
        final String schemaName = schema.getName();
        final List<RestLink> columnsLinks = Arrays.stream(table.getColumnNames()).map(c -> new RestLink(String.valueOf(c),
                uriBuilder.build(tenantIdentifier, dataContextId, schemaName, tableName, c))).collect(Collectors
                        .toList());

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "table");
        map.put("name", tableName);
        map.put("schema", schemaName);
        map.put("data-context", dataContextId);
        map.put("tenant", tenantIdentifier);
        map.put("columns", columnsLinks);
        return map;
    }
}
