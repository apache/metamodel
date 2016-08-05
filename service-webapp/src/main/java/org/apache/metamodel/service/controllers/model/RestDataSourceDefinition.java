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
package org.apache.metamodel.service.controllers.model;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.metamodel.service.app.DataSourceDefinition;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RestDataSourceDefinition implements DataSourceDefinition {

    private final Map<String, Object> properties = new HashMap<>();

    @JsonProperty(value = "type", required = true)
    @NotNull
    private String type;

    @Override
    public String getType() {
        return type;
    }

    @JsonAnyGetter
    @Override
    public Map<String, Object> getProperties() {
        return properties;
    }

    @JsonAnySetter
    public void set(String name, Object value) {
        properties.put(name, value);
    }
}
