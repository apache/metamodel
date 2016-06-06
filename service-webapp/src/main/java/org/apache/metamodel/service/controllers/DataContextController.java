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

import javax.validation.Valid;
import javax.ws.rs.core.UriBuilder;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.service.app.TenantContext;
import org.apache.metamodel.service.app.TenantRegistry;
import org.apache.metamodel.service.controllers.model.RestLink;
import org.apache.metamodel.service.controllers.model.RestDataContextDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/{tenant}/{dataContext}", produces = MediaType.APPLICATION_JSON_VALUE)
public class DataContextController {

    private final TenantRegistry tenantRegistry;

    @Autowired
    public DataContextController(TenantRegistry tenantRegistry) {
        this.tenantRegistry = tenantRegistry;
    }

    @RequestMapping(method = RequestMethod.PUT)
    @ResponseBody
    public Map<String, Object> put(@PathVariable("tenant") String tenantId,
            @PathVariable("dataContext") String dataContextId,
            @Valid @RequestBody RestDataContextDefinition dataContextDefinition) {
        final String dataContextIdentifier = tenantRegistry.getTenantContext(tenantId).getDataContextRegistry()
                .registerDataContext(dataContextId, dataContextDefinition);

        return get(tenantId, dataContextIdentifier);
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> get(@PathVariable("tenant") String tenantId,
            @PathVariable("dataContext") String dataContextId) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);
        final DataContext dataContext = tenantContext.getDataContextRegistry().openDataContext(dataContextId);

        final String tenantIdentifier = tenantContext.getTenantIdentifier();
        final UriBuilder uriBuilder = UriBuilder.fromPath("/{tenant}/{dataContext}/schemas/{schema}");

        final List<RestLink> schemaLinks = Arrays.stream(dataContext.getSchemaNames()).map(s -> new RestLink(s, uriBuilder
                .build(tenantIdentifier, dataContextId, s))).collect(Collectors.toList());

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "data-context");
        map.put("name", dataContextId);
        map.put("tenant", tenantIdentifier);
        map.put("query", UriBuilder.fromPath("/{tenant}/{dataContext}/query").build(tenantIdentifier, dataContextId));
        map.put("schemas", schemaLinks);
        return map;
    }
}
