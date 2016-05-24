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

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockServletContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RootInformationControllerTest {

    private MockMvc mockMvc;

    @Before
    public void init() {
        final RootInformationController controller = new RootInformationController();
        controller.servletContext = new MockServletContext();
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void testGenericMessageSuccess() throws Exception {
        final MockHttpServletRequestBuilder request = MockMvcRequestBuilders.get("/").contentType(
                MediaType.APPLICATION_JSON);

        final MvcResult result = mockMvc.perform(request).andExpect(MockMvcResultMatchers.status().is(200)).andReturn();

        final String content = result.getResponse().getContentAsString();

        final Map<?, ?> map = new ObjectMapper().readValue(content, Map.class);
        assertEquals("pong!", map.get("ping"));
    }
}
