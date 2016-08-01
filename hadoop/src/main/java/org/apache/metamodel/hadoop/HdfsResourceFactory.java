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
package org.apache.metamodel.hadoop;

import org.apache.metamodel.factory.ResourceFactory;
import org.apache.metamodel.factory.ResourceProperties;
import org.apache.metamodel.factory.UnsupportedResourcePropertiesException;
import org.apache.metamodel.util.HdfsResource;
import org.apache.metamodel.util.Resource;

public class HdfsResourceFactory implements ResourceFactory {

    /**
     * Property that can be used
     */
    public static final String PROPERTY_HADOOP_CONF_DIR = "hadoop-conf-dir";

    @Override
    public boolean accepts(ResourceProperties properties) {
        return "hdfs".equals(properties.getUri().getScheme());
    }

    @Override
    public Resource create(ResourceProperties properties) throws UnsupportedResourcePropertiesException {
        final Object hadoopConfDirProperty = properties.toMap().get(PROPERTY_HADOOP_CONF_DIR);
        final String hadoopConfDir = hadoopConfDirProperty == null ? null : hadoopConfDirProperty.toString();
        return new HdfsResource(properties.getUri().toString(), hadoopConfDir);
    }

}
