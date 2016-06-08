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
import java.util.Map.Entry;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateSummary;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.insert.InsertInto;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.service.app.TenantContext;
import org.apache.metamodel.service.app.TenantRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = { "/{tenant}/{dataContext}/schemas/{schema}/tables/{table}/data",
        "/{tenant}/{dataContext}/s/{schema}/t/{table}/d" }, produces = MediaType.APPLICATION_JSON_VALUE)
public class TableDataController {

    private final TenantRegistry tenantRegistry;

    @Autowired
    public TableDataController(TenantRegistry tenantRegistry) {
        this.tenantRegistry = tenantRegistry;
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> get(@PathVariable("tenant") String tenantId,
            @PathVariable("dataContext") String dataSourceName, @PathVariable("schema") String schemaId,
            @PathVariable("table") String tableId, @RequestParam(value = "offset", required = false) Integer offset,
            @RequestParam(value = "limit", required = false) Integer limit) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);
        final DataContext dataContext = tenantContext.getDataSourceRegistry().openDataContext(dataSourceName);

        final Schema schema = dataContext.getSchemaByName(schemaId);
        final Table table = schema.getTableByName(tableId);

        final Query query = dataContext.query().from(table).selectAll().toQuery();

        return QueryController.executeQuery(dataContext, query, offset, limit);
    }

    @RequestMapping(method = RequestMethod.POST)
    @ResponseBody
    public Map<String, Object> post(@PathVariable("tenant") String tenantId,
            @PathVariable("dataContext") String dataSourceName, @PathVariable("schema") String schemaId,
            @PathVariable("table") String tableId, @RequestBody Map<String, Object> inputMap) {

        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);
        final DataContext dataContext = tenantContext.getDataSourceRegistry().openDataContext(dataSourceName);
        if (!(dataContext instanceof UpdateableDataContext)) {
            throw new UnsupportedOperationException("Cannot perform updates on read-only datasource: "
                    + dataSourceName);
        }

        final Schema schema = dataContext.getSchemaByName(schemaId);
        final Table table = schema.getTableByName(tableId);

        final InsertInto insert = new InsertInto(table);
        for (Entry<String, Object> entry : inputMap.entrySet()) {
            insert.value(entry.getKey(), entry.getValue());
        }

        final UpdateableDataContext updateableDataContext = (UpdateableDataContext) dataContext;
        final UpdateSummary result = updateableDataContext.executeUpdate(insert);

        final Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "ok");

        if (result.getInsertedRows().isPresent()) {
            response.put("inserted-rows", result.getInsertedRows().get());
        }
        if (result.getGeneratedKeys().isPresent()) {
            response.put("generated-keys", result.getGeneratedKeys().get());
        }

        return response;
    }
}
