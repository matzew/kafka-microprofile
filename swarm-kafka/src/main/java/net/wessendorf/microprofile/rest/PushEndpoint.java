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
package net.wessendorf.microprofile.rest;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

import com.relayrides.pushy.apns.util.ApnsPayloadBuilder;
import net.wessendorf.microprofile.service.PushSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/push")
public class PushEndpoint {

    private final Logger logger = LoggerFactory.getLogger(PushEndpoint.class);

    @Inject
    private PushSender sender;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response processPushRequest(final Map<String, Map<String, String>> pushMsg) {

        final String payload = createPushPayload(pushMsg.get("message").get("alert"), pushMsg.get("message").get("sound"));

        logger.info("Submitted Push request to APNs for further processing....");
        // ship it:
        sender.sendPushs(payload);

        // 202 is enough - we have no guarantee anyways...
        return Response.accepted().build();
    }




    // ----------------------------- helper ---------------

    private String createPushPayload(final String message, final String sound) {
        final ApnsPayloadBuilder payloadBuilder = new ApnsPayloadBuilder();

        // only set badge if needed/included in user's payload
        payloadBuilder
                .setAlertBody(message)
                .setSoundFileName(sound);

        return payloadBuilder.buildWithDefaultMaximumLength();
    }
}
