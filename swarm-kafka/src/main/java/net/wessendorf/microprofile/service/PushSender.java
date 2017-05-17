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
package net.wessendorf.microprofile.service;

import com.relayrides.pushy.apns.ApnsClient;
import com.relayrides.pushy.apns.ApnsClientBuilder;
import com.relayrides.pushy.apns.PushNotificationResponse;
import com.relayrides.pushy.apns.util.SimpleApnsPushNotification;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import net.wessendorf.kafka.SimpleKafkaProducer;
import net.wessendorf.kafka.cdi.annotation.KafkaConfig;
import net.wessendorf.kafka.cdi.annotation.Producer;
import net.wessendorf.microprofile.apns.ServiceConstructor;
import net.wessendorf.microprofile.apns.SimpleApnsClientCache;
import net.wessendorf.microprofile.apns.TokenDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.UUID;

@KafkaConfig(bootstrapServers = "172.17.0.5:9092")
public class PushSender {

    private final Logger logger = LoggerFactory.getLogger(PushSender.class);

    @Producer(topic = "push_messages_r")
    private SimpleKafkaProducer<String, String> producer;

    @Inject
    private SimpleApnsClientCache simpleApnsClientCache;

    public void sendPushs(final String payload) {

        final List<String> tokens = TokenDB.loadDeviceTokens();
        final String pushJobID = UUID.randomUUID().toString();


        ApnsClient apnsClient = null;
        {
            try {
                apnsClient = receiveApnsConnection("net.wessendorf.something");
            } catch (IllegalArgumentException iae) {
                logger.error(iae.getMessage(), iae);
            }
        }

        if (apnsClient.isConnected()) {

            for (final String token : tokens) {

                final SimpleApnsPushNotification pushNotification = new SimpleApnsPushNotification(token, "net.wessendorf.something", payload);
                final Future<PushNotificationResponse<SimpleApnsPushNotification>> notificationSendFuture = apnsClient.sendNotification(pushNotification);

                notificationSendFuture.addListener(new GenericFutureListener<Future<? super PushNotificationResponse<SimpleApnsPushNotification>>>() {
                    @Override
                    public void operationComplete(Future<? super PushNotificationResponse<SimpleApnsPushNotification>> future) throws Exception {

                        // we could submit "something" to APNs
                        if (future.isSuccess()) {
                            handlePushNotificationResponsePerToken(pushJobID, notificationSendFuture.get());
                        }
                    }
                });
            }

        } else {
            logger.error("Unable to send notifications, client is not connected");
        }
    }



    // ----------------------------- helper ---------------

    private void handlePushNotificationResponsePerToken(final String jobID, final PushNotificationResponse<SimpleApnsPushNotification> pushNotificationResponse ) {

        final String deviceToken = pushNotificationResponse.getPushNotification().getToken();

        if (pushNotificationResponse.isAccepted()) {
            logger.trace("Push notification for '{}' (payload={})", deviceToken, pushNotificationResponse.getPushNotification().getPayload());

            producer.send(jobID, "Success");
        } else {
            final String rejectReason = pushNotificationResponse.getRejectionReason();
            logger.trace("Push Message has been rejected with reason: {}", rejectReason);


            producer.send(jobID, "Rejected");


            // token is either invalid, or did just expire
            if ((pushNotificationResponse.getTokenInvalidationTimestamp() != null) || ("BadDeviceToken".equals(rejectReason))) {
                logger.info(rejectReason + ", removing token: " + deviceToken);
            }
        }
    }


    private ApnsClient receiveApnsConnection(final String topic) {
        return simpleApnsClientCache.getApnsClientForVariant(topic, new ServiceConstructor<ApnsClient>() {
            @Override
            public ApnsClient construct() {

                final ApnsClient apnsClient = buildApnsClient();

                // connect and wait:
                connectToDestinations(apnsClient);

                // APNS client has auto-reconnect, but let's log when that happens
                apnsClient.getReconnectionFuture().addListener(new GenericFutureListener<Future<? super Void>>() {
                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {
                        logger.trace("Reconnecting to APNs");
                    }
                });
                return apnsClient;
            }
        });
    }


    private ApnsClient buildApnsClient() {

        final ApnsClient apnsClient;
        {
            try {
                apnsClient =new ApnsClientBuilder()
                        .setClientCredentials(new File("/home/Matthias/Something.p12"), "XXX")
                        .build();

                return apnsClient;

            } catch (Exception e) {
                logger.error("error construting apns client", e);
            }
        }
        // indicating an incomplete service
        throw new IllegalArgumentException("Not able to construct APNS client");
    }

    private synchronized void connectToDestinations(final ApnsClient apnsClient) {

        logger.debug("connecting to APNs");
        final Future<Void> connectFuture = apnsClient.connect(ApnsClient.DEVELOPMENT_APNS_HOST);
        try {
            connectFuture.await();
        } catch (InterruptedException e) {
            logger.error("Error connecting to APNs", e);
        }
    }

}
