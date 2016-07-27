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
import java.net.InetAddress;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RootInformationController {

    private static final Logger logger = LoggerFactory.getLogger(RootInformationController.class);

    @Autowired
    ServletContext servletContext;

    @RequestMapping(method = RequestMethod.GET, value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<String, Object> index() {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("ping", "pong!");
        map.put("application", "Apache MetaModel");
        map.put("version", getVersion());
        map.put("server-time", getServerTime());
        try {
            map.put("canonical-hostname", InetAddress.getLocalHost().getCanonicalHostName());
        } catch (Exception e) {
            logger.info("Failed to get canonical-hostname", e);
        }
        map.put("open-api", getOpenApi());
        return map;
    }

    private Map<String, Object> getOpenApi() {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("spec", servletContext.getContextPath() + "/swagger.json");
        map.put("swagger-ui", servletContext.getContextPath() + "/swagger-ui");
        return map;
    }

    private Map<String, Object> getServerTime() {
        final ZonedDateTime now = ZonedDateTime.now();
        final String dateFormatted = now.format(DateTimeFormatter.ISO_INSTANT);

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("timestamp", new Date().getTime());
        map.put("iso8601", dateFormatted);
        return map;
    }

    /**
     * Does the slightly tedious task of reading the software version from
     * META-INF based on maven metadata.
     * 
     * @return
     */
    private String getVersion() {
        final String groupId = "org.apache.metamodel";
        final String artifactId = "MetaModel-service-webapp";
        final String resourcePath = "/META-INF/maven/" + groupId + "/" + artifactId + "/pom.properties";
        final Properties properties = new Properties();
        try (final InputStream inputStream = servletContext.getResourceAsStream(resourcePath)) {
            properties.load(inputStream);
        } catch (Exception e) {
            logger.error("Failed to load version from manifest: " + e.getMessage());
        }

        final String version = properties.getProperty("version", "UNKNOWN");
        return version;
    }
}
