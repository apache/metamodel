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

import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.apache.metamodel.util.FileHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

@RestController
public class SwaggerSpecController {

    @Autowired
    ServletContext servletContext;
    
    @RequestMapping(method = RequestMethod.GET, value = "/swagger.json", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<String, Object> getSwaggerJson(HttpServletRequest req) throws Exception {
        final String yaml;
        try (final InputStream resource = getClass().getResourceAsStream("/swagger.yaml")) {
            yaml = FileHelper.readInputStreamAsString(resource, FileHelper.UTF_8_ENCODING);
        }

        final ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
        @SuppressWarnings("unchecked")
        final Map<String, Object> map = yamlReader.readValue(yaml, Map.class);

        // add the base path, scheme and host
        map.put("basePath", servletContext.getContextPath());
        map.put("host", req.getServerName() + ":" + req.getServerPort());
        map.put("schemes", Arrays.asList(req.getScheme()));

        return map;
    }
}
