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
package org.apache.metamodel.dynamodb;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.AbstractDataContextFactory;
import org.apache.metamodel.factory.DataContextFactory;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.util.SimpleTableDef;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

/**
 * {@link DataContextFactory} for DynamoDB.
 * 
 * Properties used are: username, password, region and table defs.
 */
public class DynamoDbDataContextFactory extends AbstractDataContextFactory {

    @Override
    protected String getType() {
        return "dynamodb";
    }

    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry) {
        final AmazonDynamoDBClientBuilder clientBuilder =
                AmazonDynamoDBClientBuilder.standard().withCredentials(getCredentials(properties));
        final Object region = properties.toMap().get("region");
        if (region instanceof String) {
            clientBuilder.setRegion((String) region);
        }
        
        final AmazonDynamoDB client = clientBuilder.build();
        final SimpleTableDef[] tableDefs = properties.getTableDefs();
        return new DynamoDbDataContext(client, tableDefs);
    }

    private AWSCredentialsProvider getCredentials(DataContextProperties properties) {
        if (properties.getUsername() != null) {
            final BasicAWSCredentials credentials =
                    new BasicAWSCredentials(properties.getUsername(), properties.getPassword());
            return new AWSStaticCredentialsProvider(credentials);
        } else {
            return DefaultAWSCredentialsProviderChain.getInstance();
        }
    }
}
