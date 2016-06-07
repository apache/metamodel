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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.core.UriBuilder;

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
@RequestMapping(value = "/{tenant}", produces = MediaType.APPLICATION_JSON_VALUE)
public class TenantController {

    private final TenantRegistry tenantRegistry;

    @Autowired
    public TenantController(TenantRegistry tenantRegistry) {
        this.tenantRegistry = tenantRegistry;
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> getTenant(@PathVariable("tenant") String tenantName) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantName);
        if (tenantContext == null) {
            throw new IllegalArgumentException("No such tenant: " + tenantName);
        }

        final String tenantNameNormalized = tenantContext.getTenantName();

        final UriBuilder uriBuilder = UriBuilder.fromPath("/{tenant}/{datasource}");

        final List<String> dataContextIdentifiers = tenantContext.getDataSourceRegistry().getDataSourceNames();
        final List<RestLink> dataSourceLinks = dataContextIdentifiers.stream().map(s -> new RestLink(s, uriBuilder
                .build(tenantNameNormalized, s))).collect(Collectors.toList());

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "tenant");
        map.put("name", tenantNameNormalized);
        map.put("datasources", dataSourceLinks);
        return map;
    }

    @RequestMapping(method = RequestMethod.PUT)
    @ResponseBody
    public Map<String, Object> putTenant(@PathVariable("tenant") String tenantName) {
        final TenantContext tenantContext = tenantRegistry.createTenantContext(tenantName);
        final String tenantIdentifier = tenantContext.getTenantName();

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "tenant");
        map.put("name", tenantIdentifier);

        return map;
    }

    @RequestMapping(method = RequestMethod.DELETE)
    @ResponseBody
    public Map<String, Object> deleteTenant(@PathVariable("tenant") String tenantName) {
        final boolean deleted = tenantRegistry.deleteTenantContext(tenantName);

        if (!deleted) {
            throw new IllegalArgumentException("No such tenant: " + tenantName);
        }

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "tenant");
        map.put("name", tenantName);
        map.put("deleted", deleted);

        return map;
    }
}
