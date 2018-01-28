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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Test;

public class KafkaDataContextTest extends EasyMockSupport {

    @Test
    public void testGetSchemaInfo() {
        final ConsumerAndProducerFactory consumerFactory = createMock(ConsumerAndProducerFactory.class);

        replayAll();

        final Supplier<Collection<String>> topicSupplier = () -> Arrays.asList("foo", "bar");
        final DataContext dc = new KafkaDataContext<>(String.class, String.class, consumerFactory, topicSupplier);

        verifyAll();

        Assert.assertEquals("[foo, bar]", dc.getDefaultSchema().getTableNames().toString());
    }

    @Test
    public void testQueryWithoutOptimization() {
        final ConsumerAndProducerFactory consumerFactory = createMock(ConsumerAndProducerFactory.class);
        @SuppressWarnings("unchecked")
        final Consumer<String, String> consumer = createMock(Consumer.class);

        EasyMock.expect(consumerFactory.createConsumer("myTopic", String.class, String.class)).andReturn(consumer);

        final List<PartitionInfo> partitionInfoList = new ArrayList<>();
        partitionInfoList.add(new PartitionInfo("myTopic", 0, null, null, null));
        partitionInfoList.add(new PartitionInfo("myTopic", 1, null, null, null));
        partitionInfoList.add(new PartitionInfo("myTopic", 2, null, null, null));
        partitionInfoList.add(new PartitionInfo("myTopic", 3, null, null, null));

        EasyMock.expect(consumer.partitionsFor("myTopic")).andReturn(partitionInfoList);

        final Capture<Collection<TopicPartition>> assignmentCapture = new Capture<>();
        consumer.assign(EasyMock.capture(assignmentCapture));
        consumer.seekToBeginning(EasyMock.anyObject());

        final ConsumerRecords<String, String> consumerRecords1 = createConsumerRecords(1, 0, 10);
        final ConsumerRecords<String, String> consumerRecords2 = createConsumerRecords(2, 20, 10);

        EasyMock.expect(consumer.poll(1000)).andReturn(consumerRecords1);
        EasyMock.expect(consumer.poll(1000)).andReturn(consumerRecords2);
        EasyMock.expect(consumer.poll(1000)).andReturn(null);

        consumer.unsubscribe();
        consumer.close();

        replayAll();

        final Supplier<Collection<String>> topicSupplier = () -> Arrays.asList("myTopic");
        final DataContext dc = new KafkaDataContext<>(String.class, String.class, consumerFactory, topicSupplier);

        final DataSet dataSet = dc.query().from("myTopic").select("partition", "offset", "value").where("key").eq(
                "key2").execute();
        Assert.assertTrue(dataSet.next());
        Assert.assertEquals("Row[values=[1, 2, value2]]", dataSet.getRow().toString());
        Assert.assertTrue(dataSet.next());
        Assert.assertEquals("Row[values=[2, 22, value2]]", dataSet.getRow().toString());
        Assert.assertFalse(dataSet.next());
        dataSet.close();

        verifyAll();

        // assert that we assigned exactly the three partitions we queried for
        final ArrayList<TopicPartition> capturedAssignmentList = new ArrayList<>(assignmentCapture.getValue());
        Assert.assertEquals(4, capturedAssignmentList.size());
        Assert.assertEquals(0, capturedAssignmentList.get(0).partition());
        Assert.assertEquals(1, capturedAssignmentList.get(1).partition());
        Assert.assertEquals(2, capturedAssignmentList.get(2).partition());
        Assert.assertEquals(3, capturedAssignmentList.get(3).partition());
    }

    @Test
    public void testQueryOptimizationByPartition() {
        final ConsumerAndProducerFactory consumerFactory = createMock(ConsumerAndProducerFactory.class);
        @SuppressWarnings("unchecked")
        final Consumer<String, String> consumer = createMock(Consumer.class);

        EasyMock.expect(consumerFactory.createConsumer("myTopic", String.class, String.class)).andReturn(consumer);

        final Capture<Collection<TopicPartition>> assignmentCapture = new Capture<>();
        consumer.assign(EasyMock.capture(assignmentCapture));
        consumer.seekToBeginning(EasyMock.anyObject());

        final ConsumerRecords<String, String> consumerRecords1 = createConsumerRecords(1, 0, 2);
        final ConsumerRecords<String, String> consumerRecords2 = createConsumerRecords(2, 20, 1);

        EasyMock.expect(consumer.poll(1000)).andReturn(consumerRecords1);
        EasyMock.expect(consumer.poll(1000)).andReturn(consumerRecords2);
        EasyMock.expect(consumer.poll(1000)).andReturn(null);

        consumer.unsubscribe();
        consumer.close();

        replayAll();

        final Supplier<Collection<String>> topicSupplier = () -> Arrays.asList("myTopic");
        final DataContext dc = new KafkaDataContext<>(String.class, String.class, consumerFactory, topicSupplier);

        final DataSet dataSet = dc.query().from("myTopic").select("offset", "value").where("partition").in(1, 2, 42)
                .execute();
        Assert.assertTrue(dataSet.next());
        Assert.assertEquals("Row[values=[0, value0]]", dataSet.getRow().toString());
        Assert.assertTrue(dataSet.next());
        Assert.assertEquals("Row[values=[1, value1]]", dataSet.getRow().toString());
        Assert.assertTrue(dataSet.next());
        Assert.assertEquals("Row[values=[20, value0]]", dataSet.getRow().toString());
        Assert.assertFalse(dataSet.next());
        dataSet.close();

        verifyAll();

        // assert that we assigned exactly the three partitions we queried for
        final ArrayList<TopicPartition> capturedAssignmentList = new ArrayList<>(assignmentCapture.getValue());
        Assert.assertEquals(3, capturedAssignmentList.size());
        Assert.assertEquals(1, capturedAssignmentList.get(0).partition());
        Assert.assertEquals(2, capturedAssignmentList.get(1).partition());
        Assert.assertEquals(42, capturedAssignmentList.get(2).partition());
    }

    private ConsumerRecords<String, String> createConsumerRecords(int partition, int offset, int howMany) {
        final List<ConsumerRecord<String, String>> list = new ArrayList<>();
        for (int i = 0; i < howMany; i++) {
            list.add(new ConsumerRecord<String, String>("myTopic", partition, offset + i, "key" + i, "value" + i));
        }

        final Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
        records.put(new TopicPartition("myTopic", partition), list);
        return new ConsumerRecords<>(records);
    }

}
