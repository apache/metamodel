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

import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;

/**
 * Default {@link ConsumerFactory} implementation.
 */
public class KafkaConsumerFactory implements ConsumerFactory {

    private final Properties baseProperties;

    public KafkaConsumerFactory(String bootstrapServers) {
        this.baseProperties = new Properties();
        this.baseProperties.setProperty("bootstrap.servers", bootstrapServers);
    }

    public KafkaConsumerFactory(Properties baseProperties) {
        this.baseProperties = baseProperties;
    }

    @Override
    public <K, V> Consumer<K, V> createConsumer(String topic, Class<K> keyClass, Class<V> valueClass) {
        final String groupId = "apache_metamodel_" + topic + "_" + System.currentTimeMillis();

        final Properties properties = new Properties();
        baseProperties.stringPropertyNames().forEach(k -> {
            properties.setProperty(k, baseProperties.getProperty(k));
        });

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerForClass(keyClass).getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerForClass(keyClass)
                .getName());
        return new KafkaConsumer<>(properties);
    }

    private static Class<? extends Deserializer<?>> deserializerForClass(Class<?> cls) {
        if (cls == String.class || cls == CharSequence.class) {
            return StringDeserializer.class;
        }
        if (cls == Double.class) {
            return DoubleDeserializer.class;
        }
        if (cls == Integer.class) {
            return IntegerDeserializer.class;
        }
        if (cls == Float.class) {
            return FloatDeserializer.class;
        }
        if (cls == Long.class) {
            return LongDeserializer.class;
        }
        if (cls == Short.class) {
            return ShortDeserializer.class;
        }
        if (cls == Bytes.class) {
            return BytesDeserializer.class;
        }
        if (cls == ByteBuffer.class) {
            return ByteBufferDeserializer.class;
        }
        if (cls == byte[].class || cls == Byte[].class || cls == Object.class) {
            return ByteArrayDeserializer.class;
        }
        // fall back to doing nothing
        return ByteArrayDeserializer.class;
    }

}
