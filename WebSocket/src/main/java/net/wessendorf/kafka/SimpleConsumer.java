/**
 * Copyright (C) 2017 Matthias Wessendorf.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.wessendorf.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class SimpleConsumer implements Runnable {


    final Properties properties = new Properties();
    KafkaConsumer<String, String> consumer;
    private final static Logger logger = Logger.getLogger(SimpleConsumer.class.getName());



    public SimpleConsumer() {
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "172.17.0.3:9092");
        properties.put(GROUP_ID_CONFIG, "foo_bar");
        properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer(properties);
    }

    @Override
    public void run() {

//        consumer.subscribe(Arrays.asList("websocket_bridge"));

//        consumer.subscribe(Arrays.asList("websocket_bridge"), new ConsumerRebalanceListener() {
//            @Override
//            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//                logger.severe(partitions+"");
//            }
//
//            @Override
//            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//                logger.severe(partitions+"");
//                consumer.seekToBeginning(partitions);
//            }
//        });



        TopicPartition topicPartition = new TopicPartition("websocket_bridge", 0);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToBeginning(Arrays.asList(topicPartition));

        logger.warning("Done w/ subscribing");

        boolean running = true;
        while (running) {
            final ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {

                logger.severe("got: " + record.value() + " from: " + new Date(record.timestamp()));

            }
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }


    public static void main(String... args ) throws Exception {

        final List<SimpleConsumer> consumers = new ArrayList<>();
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        final SimpleConsumer simpleConsumer = new SimpleConsumer();
        consumers.add(simpleConsumer);
        executor.submit(simpleConsumer);


    }


}
