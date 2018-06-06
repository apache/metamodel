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
package org.apache.metamodel.kafka;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class KafkaTestServer {

    private final boolean _configured;
    private String _bootstrapServers;
    private String _topicPrefix;

    public KafkaTestServer() {
        final File file = new File(getPropertyFilePath());
        if (file.exists()) {
            _configured = loadPropertyFile(file);
        } else {
            // Continuous integration case
            if (System.getenv("CONTINUOUS_INTEGRATION") != null) {
                File travisFile = new File("../travis-metamodel-integrationtest-configuration.properties");
                if (travisFile.exists()) {
                    _configured = loadPropertyFile(travisFile);
                } else {
                    _configured = false;
                }
            } else {
                _configured = false;
            }
        }
    }

    public boolean isConfigured() {
        return _configured;
    }

    public String getBootstrapServers() {
        return _bootstrapServers;
    }

    public String getTopicPrefix() {
        return _topicPrefix;
    }

    private boolean loadPropertyFile(File file) {
        final Properties properties = new Properties();
        try {
            properties.load(new FileReader(file));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        _bootstrapServers = properties.getProperty("kafka.bootstrap.servers");
        _topicPrefix = properties.getProperty("kafka.topic.prefix");

        final boolean configured = (_bootstrapServers != null && !_bootstrapServers.isEmpty() && _topicPrefix != null
                && !_topicPrefix.isEmpty());

        if (configured) {
            System.out.println("Loaded Kafka configuration. BootstrapServers=" + _bootstrapServers + ", TopicPrefix="
                    + _topicPrefix);
        }
        return configured;
    }

    private String getPropertyFilePath() {
        String userHome = System.getProperty("user.home");
        return userHome + "/metamodel-integrationtest-configuration.properties";
    }

    public String getInvalidConfigurationMessage() {
        return "!!! WARN !!! Kafka module ignored\r\n" + "Please configure kafka connection locally ("
                + getPropertyFilePath() + "), to run integration tests";
    }
}
