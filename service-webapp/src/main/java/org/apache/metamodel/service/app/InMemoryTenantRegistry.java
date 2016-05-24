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

/**
 * In-memory {@link TenantRegistry}. This is not particularly
 * production-friendly as it is non-persistent, but it is useful for demo
 * purposes.
 */
public class InMemoryTenantRegistry implements TenantRegistry {

    private final Map<String, TenantContext> tenants;

    public InMemoryTenantRegistry() {
        tenants = new LinkedHashMap<>();
    }

    @Override
    public List<String> getTenantIdentifiers() {
        return tenants.keySet().stream().collect(Collectors.toList());
    }

    @Override
    public TenantContext getTenantContext(String tenantIdentifier) {
        return tenants.get(tenantIdentifier);
    }

    @Override
    public TenantContext createTenantContext(String tenantIdentifier) {
        if (tenants.containsKey(tenantIdentifier)) {
            throw new IllegalArgumentException("Tenant already exist: " + tenantIdentifier);
        }
        final InMemoryTenantContext tenantContext = new InMemoryTenantContext(tenantIdentifier);
        tenants.put(tenantIdentifier, tenantContext);
        return tenantContext;
    }

    @Override
    public boolean deleteTenantContext(String tenantIdentifier) {
        return tenants.remove(tenantIdentifier) != null;
    }

}
