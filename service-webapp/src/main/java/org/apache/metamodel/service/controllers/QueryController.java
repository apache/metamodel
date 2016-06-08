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
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.service.app.TenantContext;
import org.apache.metamodel.service.app.TenantRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = { "/{tenant}/{dataContext}/query",
        "/{tenant}/{dataContext}/q" }, produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryController {

    private final TenantRegistry tenantRegistry;

    @Autowired
    public QueryController(TenantRegistry tenantRegistry) {
        this.tenantRegistry = tenantRegistry;
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> get(@PathVariable("tenant") String tenantId,
            @PathVariable("dataContext") String dataSourceName,
            @RequestParam(value = "sql", required = true) String queryString,
            @RequestParam(value = "offset", required = false) Integer offset,
            @RequestParam(value = "limit", required = false) Integer limit) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);
        final DataContext dataContext = tenantContext.getDataSourceRegistry().openDataContext(dataSourceName);

        final Query query = dataContext.parseQuery(queryString);
        if (offset != null) {
            query.setFirstRow(offset);
        }
        if (limit != null) {
            query.setMaxRows(limit);
        }

        final DataSet dataSet = dataContext.executeQuery(query);

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "dataset");
        map.put("header", Arrays.stream(dataSet.getSelectItems()).map((si) -> si.toString()).collect(Collectors
                .toList()));
        map.put("data", dataSet.toObjectArrays());
        return map;
    }
}
