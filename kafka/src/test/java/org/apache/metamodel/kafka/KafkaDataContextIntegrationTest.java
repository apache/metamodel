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

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.WrappingDataSet;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaDataContextIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(KafkaDataContextIntegrationTest.class);

    private final KafkaTestServer testServer = new KafkaTestServer();

    @Before
    public void setUp() {
        final boolean configured = testServer.isConfigured();
        if (!configured) {
            System.err.println(testServer.getInvalidConfigurationMessage());
        }
        Assume.assumeTrue(configured);
    }

    @Test
    public void testGetSchemaInfo() {
        final DataContext dataContext1 = new KafkaDataContext<>(String.class, String.class, testServer
                .getBootstrapServers(), Arrays.asList("non-existing-topic"));

        Assert.assertEquals("[non-existing-topic, default_table]", dataContext1.getDefaultSchema().getTableNames()
                .toString());

        final DataContext dataContext2 = new KafkaDataContext<>(String.class, String.class, testServer
                .getBootstrapServers(), Arrays.asList("test1", "test2", "test3"));
        Assert.assertEquals("[test1, test2, test3]", dataContext2.getDefaultSchema().getTableNames().toString());
    }

    @Test
    public void testQueryNoFilters() {
        final String topic = testServer.getTopicPrefix() + UUID.randomUUID().toString().replaceAll("\\-", "");

        final DataContext dataContext = new KafkaDataContext<>(String.class, String.class, testServer
                .getBootstrapServers(), Arrays.asList(topic));

        Assert.assertEquals("[" + topic + ", default_table]", dataContext.getDefaultSchema().getTableNames()
                .toString());

        final int numRecords = 10000;

        // create a producer thread
        new Thread(createProducerRunnable(topic, numRecords), "producer").start();

        int counter = 0;
        try (DataSet dataSet = dataContext.query().from(topic).selectAll().execute()) {
            logger.info("c: starting");
            while (dataSet.next()) {
                counter++;
                if (counter % 1000 == 0) {
                    logger.info("c: " + counter);
                    logger.info(dataSet.getRow().toString());
                }
            }
            logger.info("c: done - " + counter);
        }

        Assert.assertEquals(numRecords, counter);
    }

    @Test
    public void testQueryUsingOffset() throws InterruptedException {
        final String topic = testServer.getTopicPrefix() + UUID.randomUUID().toString().replaceAll("\\-", "");

        final DataContext dataContext = new KafkaDataContext<>(String.class, String.class, testServer
                .getBootstrapServers(), Arrays.asList(topic));

        final int numRecords = 1000;
        final int queriedOffset = 500;

        // create a producer thread
        final Thread thread = new Thread(createProducerRunnable(topic, numRecords), "producer");
        thread.start();
        thread.join(); // await completion so that the queried offset will exist at query time

        int counter = 0;
        try (DataSet dataSet = dataContext.query().from(topic).selectAll().where("offset").gt(queriedOffset)
                .execute()) {

            // check the assignment and position created for the consumer
            @SuppressWarnings("resource")
            DataSet innerDataSet = dataSet;
            if (innerDataSet instanceof WrappingDataSet) {
                innerDataSet = ((WrappingDataSet) dataSet).getWrappedDataSet();
            }
            Assert.assertTrue(innerDataSet instanceof KafkaDataSet);
            final Consumer<?, ?> consumer = ((KafkaDataSet<?, ?>) innerDataSet).getConsumer();
            final Set<TopicPartition> assignment = consumer.assignment();
            for (TopicPartition assignedTopic : assignment) {
                final long position = consumer.position(assignedTopic);
                Assert.assertEquals(queriedOffset + 1, position);
            }

            while (dataSet.next()) {
                counter++;
            }
        }

        // offset is 0 based, so "greater than 500" will leave 499 records
        Assert.assertEquals(499, counter);
    }

    private Runnable createProducerRunnable(String topic, int numRecords) {
        return new Runnable() {
            @Override
            public void run() {
                final Properties properties = new Properties();
                properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testServer.getBootstrapServers());
                properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "metamodel-test");

                final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
                int counter = 0;
                while (counter < numRecords) {
                    final String key = UUID.randomUUID().toString();
                    final String value = UUID.randomUUID().toString();
                    try {
                        producer.send(new ProducerRecord<String, String>(topic, key, value), new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if (exception != null) {
                                    logger.info("Callback error");
                                    exception.printStackTrace();
                                }
                            }
                        });
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }
                    if (counter % 1000 == 0) {
                        logger.info("p: " + counter);
                    }
                    counter++;
                }
                logger.info("p: closing - " + counter);
                producer.flush();
                producer.close();
                logger.info("p: done - " + counter);
            }
        };
    }
}
