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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.validation.Valid;
import javax.ws.rs.core.UriBuilder;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.DataContextPropertiesImpl;
import org.apache.metamodel.service.app.TenantContext;
import org.apache.metamodel.service.app.TenantRegistry;
import org.apache.metamodel.service.controllers.model.RestDataSourceDefinition;
import org.apache.metamodel.service.controllers.model.RestLink;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/{tenant}/{datasource}", produces = MediaType.APPLICATION_JSON_VALUE)
public class DataSourceController {

    private final TenantRegistry tenantRegistry;

    @Autowired
    public DataSourceController(TenantRegistry tenantRegistry) {
        this.tenantRegistry = tenantRegistry;
    }

    @RequestMapping(method = RequestMethod.PUT)
    @ResponseBody
    public Map<String, Object> put(@PathVariable("tenant") String tenantId,
            @PathVariable("datasource") String dataSourceId,
            @Valid @RequestBody RestDataSourceDefinition dataContextDefinition) {

        final Map<String, Object> map = new HashMap<>();
        map.putAll(dataContextDefinition.getProperties());
        map.put(DataContextPropertiesImpl.PROPERTY_DATA_CONTEXT_TYPE, dataContextDefinition.getType());

        if (!map.containsKey(DataContextPropertiesImpl.PROPERTY_DATABASE)) {
            // add the data source ID as database name if it is not already set.
            map.put(DataContextPropertiesImpl.PROPERTY_DATABASE, dataSourceId);
        }

        final DataContextProperties properties = new DataContextPropertiesImpl(map);

        final String dataContextIdentifier = tenantRegistry.getTenantContext(tenantId).getDataSourceRegistry()
                .registerDataSource(dataSourceId, properties);

        return get(tenantId, dataContextIdentifier);
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> get(@PathVariable("tenant") String tenantId,
            @PathVariable("datasource") String dataSourceName) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantId);
        final DataContext dataContext = tenantContext.getDataSourceRegistry().openDataContext(dataSourceName);

        final String tenantName = tenantContext.getTenantName();
        final UriBuilder uriBuilder = UriBuilder.fromPath("/{tenant}/{dataContext}/s/{schema}");

        final List<RestLink> schemaLinks = Arrays.stream(dataContext.getSchemaNames()).map(s -> new RestLink(s,
                uriBuilder.build(tenantName, dataSourceName, s))).collect(Collectors.toList());

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "datasource");
        map.put("name", dataSourceName);
        map.put("tenant", tenantName);
        map.put("updateable", dataContext instanceof UpdateableDataContext);
        map.put("query", UriBuilder.fromPath("/{tenant}/{dataContext}/query").build(tenantName, dataSourceName));
        map.put("schemas", schemaLinks);
        return map;
    }
}
