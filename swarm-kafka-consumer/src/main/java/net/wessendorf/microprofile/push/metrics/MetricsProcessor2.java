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
package net.wessendorf.microprofile.push.metrics;

import net.wessendorf.kafka.cdi.annotation.Consumer;
import net.wessendorf.kafka.cdi.annotation.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KafkaConfig(bootstrapServers = "172.17.0.5:9092")
public class MetricsProcessor2 {

    private final Logger logger = LoggerFactory.getLogger(MetricsProcessor2.class);

    @Consumer(topic = "successMessagesPerJob", groupId = "successMessagesPerJobProcessor")
    public void successMessagesPerJob(final Long metricPerAppleDevice) {

        logger.info("successMessagesPerJob: {}", metricPerAppleDevice);
    }
    @Consumer(topic = "failedMessagesPerJob", groupId = "failedMessagesPerJobProcessor")
    public void failedMessagesPerJob(final Long metricPerAppleDevice) {

        logger.info("failedMessagesPerJob: {}", metricPerAppleDevice);
    }
    @Consumer(topic = "totalMessagesPerJob", groupId = "totalMessagesPerJobProcessor")
    public void totalMessagesPerJob(final Long metricPerAppleDevice) {

        logger.info("totalMessagesPerJob: {}", metricPerAppleDevice);
    }
}
