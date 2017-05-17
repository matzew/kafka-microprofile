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
package net.wessendorf.microprofile.kafka.cdi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class StreamHelper {

    static Logger logger = LoggerFactory.getLogger(StreamHelper.class);

    public static void main(String... args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.5:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        // read from the topic that contains all messages, for all jobs
        final KStream<String, String> source = builder.stream("push_messages_r");


        // some simple processing, and grouping by key, applying a predicate and send to new topic:

        final KTable<String, Long> successCountsPerJob = source.filter((key, value) -> value.equals("Success"))
                .groupByKey()
                .count("successMessagesPerJob");
        successCountsPerJob.to(Serdes.String(), Serdes.Long(), "successMessagesPerJob");

        final KTable<String, Long> failCountsPerJob = source.filter((key, value) -> value.equals("Rejected"))
                .groupByKey()
                .count("failedMessagesPerJob");
        failCountsPerJob.to(Serdes.String(), Serdes.Long(), "failedMessagesPerJob");

        source.groupByKey()
                .count("totalMessagesPerJob")
                .to(Serdes.String(), Serdes.Long(), "totalMessagesPerJob");


//        // we also could branch, since we all want to
//        final KStream<String, Long>[] branches = source.branch(
//                (key, value) -> value.equals("Success"),
//                (key, value) -> value.equals("Rejected"),
//                (key, value) -> true
//        );
//        // sample: just done the above, only on first
//        branches[0].groupByKey().count("successMessagesPerJob").to(Serdes.String(), Serdes.Long(), "successMessagesPerJob");
//
//
//

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
    }
}
