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
package org.apache.metamodel.service.app;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.metamodel.service.app.exceptions.NoSuchTenantException;
import org.apache.metamodel.service.app.exceptions.TenantAlreadyExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory {@link TenantRegistry}. This is not particularly
 * production-friendly as it is non-persistent, but it is useful for demo
 * purposes.
 */
public class InMemoryTenantRegistry implements TenantRegistry {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryTenantRegistry.class);
    private final Map<String, TenantContext> tenants;

    public InMemoryTenantRegistry() {
        tenants = new LinkedHashMap<>();
        logger.info("Initialized!");
    }

    @Override
    public List<String> getTenantIdentifiers() {
        return tenants.keySet().stream().collect(Collectors.toList());
    }

    @Override
    public TenantContext getTenantContext(String tenantIdentifier) {
        final TenantContext tenant = tenants.get(tenantIdentifier);
        if (tenant == null) {
            throw new NoSuchTenantException(tenantIdentifier);
        }
        return tenant;
    }

    @Override
    public TenantContext createTenantContext(String tenantIdentifier) {
        if (tenants.containsKey(tenantIdentifier)) {
            throw new TenantAlreadyExistException(tenantIdentifier);
        }
        final InMemoryTenantContext tenantContext = new InMemoryTenantContext(tenantIdentifier);
        tenants.put(tenantIdentifier, tenantContext);
        logger.info("Created new tenant: {}", tenantContext);
        return tenantContext;
    }

    @Override
    public void deleteTenantContext(String tenantIdentifier) {
        final TenantContext removedTenant = tenants.remove(tenantIdentifier);
        if (removedTenant == null) {
            throw new NoSuchTenantException(tenantIdentifier);
        }
    }

}
