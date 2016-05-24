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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.UriBuilder;

import org.apache.metamodel.service.app.TenantContext;
import org.apache.metamodel.service.app.TenantRegistry;
import org.apache.metamodel.service.controllers.model.Link;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpServerErrorException;

@RestController
@RequestMapping(value = "/{tenant}", produces = MediaType.APPLICATION_JSON_VALUE)
public class TenantController {

    @Autowired
    TenantRegistry tenantRegistry;

    @RequestMapping(method = RequestMethod.PUT)
    @ResponseBody
    public Map<String, Object> putTenant(@PathVariable("tenant") String tenantName) {
        final TenantContext tenantContext;
        try {
            tenantContext = tenantRegistry.createTenantContext(tenantName);
        } catch (IllegalArgumentException e) {
            throw new HttpServerErrorException(HttpStatus.CONFLICT, e.getMessage());
        }
        final String tenantIdentifier = tenantContext.getTenantIdentifier();

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "tenant");
        map.put("id", tenantIdentifier);

        return map;
    }

    @RequestMapping(method = RequestMethod.DELETE)
    @ResponseBody
    public Map<String, Object> deleteTenant(@PathVariable("tenant") String tenantName) {
        final boolean deleted = tenantRegistry.deleteTenantContext(tenantName);

        if (!deleted) {
            throw new HttpServerErrorException(HttpStatus.NOT_FOUND, "No such tenant: " + tenantName);
        }

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "tenant");
        map.put("id", tenantName);
        map.put("deleted", deleted);

        return map;
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Object> getTenant(@PathVariable("tenant") String tenantName) {
        final TenantContext tenantContext = tenantRegistry.getTenantContext(tenantName);
        if (tenantContext == null) {
            throw new HttpServerErrorException(HttpStatus.NOT_FOUND, "No such tenant: " + tenantName);
        }

        final String tenantIdentifier = tenantContext.getTenantIdentifier();

        final UriBuilder uriBuilder = UriBuilder.fromPath("/{tenant}/{dataContext}");

        final List<String> dataContextIdentifiers = tenantContext.getDataContextRegistry().getDataContextIdentifiers();
        final List<Link> dataContextLinks = dataContextIdentifiers.stream().map(s -> new Link(s, uriBuilder.build(
                tenantIdentifier, s))).collect(Collectors.toList());

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", "tenant");
        map.put("id", tenantIdentifier);
        map.put("data-contexts", dataContextLinks);
        return map;
    }

    @ExceptionHandler(HttpServerErrorException.class)
    public void handleException(HttpServerErrorException e, HttpServletResponse resp) throws IOException {
        resp.sendError(e.getStatusCode().value(), e.getStatusText());
    }
}
