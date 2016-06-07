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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Map;

import org.apache.metamodel.service.app.InMemoryTenantRegistry;
import org.apache.metamodel.service.app.TenantRegistry;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TenantInteractionScenarioTest {

    private MockMvc mockMvc;

    @Before
    public void init() {
        TenantRegistry tenantRegistry = new InMemoryTenantRegistry();
        TenantController tenantController = new TenantController(tenantRegistry);
        DataSourceController dataContextController = new DataSourceController(tenantRegistry);
        SchemaController schemaController = new SchemaController(tenantRegistry);
        TableController tableController = new TableController(tenantRegistry);
        ColumnController columnController = new ColumnController(tenantRegistry);

        mockMvc = MockMvcBuilders.standaloneSetup(tenantController, dataContextController, schemaController,
                tableController, columnController).build();
    }

    @Test
    public void testScenario() throws Exception {
        // create tenant
        {
            final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.put("/tenant1").contentType(
                    MediaType.APPLICATION_JSON)).andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

            final String content = result.getResponse().getContentAsString();
            final Map<?, ?> map = new ObjectMapper().readValue(content, Map.class);
            assertEquals("tenant", map.get("type"));
            assertEquals("tenant1", map.get("name"));
            assertNull(map.get("datasources"));
        }

        // create datasource
        {
            final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.put("/tenant1/mydata").content(
                    "{'type':'pojo'}".replace('\'', '"')).contentType(MediaType.APPLICATION_JSON)).andExpect(
                            MockMvcResultMatchers.status().isOk()).andReturn();

            final String content = result.getResponse().getContentAsString();
            final Map<?, ?> map = new ObjectMapper().readValue(content, Map.class);
            assertEquals("datasource", map.get("type"));
            assertEquals("mydata", map.get("name"));
            assertEquals(
                    "[{name=information_schema, uri=/tenant1/mydata/s/information_schema}, {name=Schema, uri=/tenant1/mydata/s/Schema}]",
                    map.get("schemas").toString());
        }

        // explore tenant - now with a datasource
        {
            final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/tenant1")).andExpect(
                    MockMvcResultMatchers.status().isOk()).andReturn();

            final String content = result.getResponse().getContentAsString();
            final Map<?, ?> map = new ObjectMapper().readValue(content, Map.class);
            assertEquals("tenant", map.get("type"));
            assertEquals("tenant1", map.get("name"));
            assertEquals("[{name=mydata, uri=/tenant1/mydata}]", map.get("datasources").toString());
        }

        // explore schema
        {
            final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/tenant1/mydata/s/information_schema"))
                    .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

            final String content = result.getResponse().getContentAsString();
            final Map<?, ?> map = new ObjectMapper().readValue(content, Map.class);
            assertEquals("schema", map.get("type"));
            assertEquals("information_schema", map.get("name"));

            assertEquals("[{name=tables, uri=/tenant1/mydata/s/information_schema/t/tables}, "
                    + "{name=columns, uri=/tenant1/mydata/s/information_schema/t/columns}, "
                    + "{name=relationships, uri=/tenant1/mydata/s/information_schema/t/relationships}]", map.get(
                            "tables").toString());
        }

        // explore table
        {
            final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get(
                    "/tenant1/mydata/s/information_schema/t/columns")).andExpect(MockMvcResultMatchers.status().isOk())
                    .andReturn();

            final String content = result.getResponse().getContentAsString();
            final Map<?, ?> map = new ObjectMapper().readValue(content, Map.class);
            assertEquals("table", map.get("type"));
            assertEquals("columns", map.get("name"));

            assertEquals("[{name=name, uri=/tenant1/mydata/s/information_schema/t/columns/c/name}, "
                    + "{name=type, uri=/tenant1/mydata/s/information_schema/t/columns/c/type}, "
                    + "{name=native_type, uri=/tenant1/mydata/s/information_schema/t/columns/c/native_type}, "
                    + "{name=size, uri=/tenant1/mydata/s/information_schema/t/columns/c/size}, "
                    + "{name=nullable, uri=/tenant1/mydata/s/information_schema/t/columns/c/nullable}, "
                    + "{name=indexed, uri=/tenant1/mydata/s/information_schema/t/columns/c/indexed}, "
                    + "{name=table, uri=/tenant1/mydata/s/information_schema/t/columns/c/table}, "
                    + "{name=remarks, uri=/tenant1/mydata/s/information_schema/t/columns/c/remarks}]", map.get(
                            "columns").toString());
        }

        // explore column
        {
            final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get(
                    "/tenant1/mydata/s/information_schema/t/columns/c/nullable")).andExpect(MockMvcResultMatchers
                            .status().isOk()).andReturn();

            final String content = result.getResponse().getContentAsString();
            final Map<?, ?> map = new ObjectMapper().readValue(content, Map.class);
            assertEquals("column", map.get("type"));
            assertEquals("nullable", map.get("name"));

            assertEquals(
                    "{number=4, size=null, nullable=true, primary-key=false, indexed=false, column-type=BOOLEAN, native-type=null, remarks=null}",
                    map.get("metadata").toString());
        }
    }
}
