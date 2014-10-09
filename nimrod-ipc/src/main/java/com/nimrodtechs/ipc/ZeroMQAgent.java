/*
 * Copyright 2014 Andrew Crutchlow
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nimrodtechs.ipc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nimrodtechs.serialization.NimrodObjectSerializer;
import com.nimrodtechs.serialization.kryo.KryoSerializer;

/**
 * Optional agent process that can be run to provide a link between running distributed processes.
 * Instantiates itself as a many-to-one subscriber and a one-to-many publisher.
 * At least two sockets are required for this operation. They are domain specific and need to be on
 * 'well-known' locations that all other processes that want to participate will know about.
 * i.e. the participants are configured with these well known values.
 * @author andy
 *
 */
public class ZeroMQAgent implements MessageReceiverInterface {
    private static Logger logger = LoggerFactory.getLogger(ZeroMQAgent.class);
    private static ZeroMQAgent zeroMQAgent = null;
    static ZeroMQPubSubSubscriber subscriber;
    static ZeroMQPubSubPublisher publisher;
    public static void main(String[] args) {
        //Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if(zeroMQAgent != null)
                    zeroMQAgent.dispose();
            }
        });
        
        //Configure the general serializer by adding a kryo serializer
        //NimrodObjectSerializer.GetInstance().getSerializers().put(NimrodObjectSerializer.DEFAULT_SERIALIZATION_ID,new KryoSerializer());
        try {
            zeroMQAgent = new ZeroMQAgent();
            
            publisher = new ZeroMQPubSubPublisher();
            publisher.setServerSocket(System.getProperty("zeroMQAgentOutboundSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/zeroMQAgentOutboundSocketUrl.pubsub"));
            publisher.setInstanceName("agentPublisher");
            publisher.initialize();
            
            subscriber = new ZeroMQPubSubSubscriber();
            subscriber.setInstanceName("agentSubscriber");
            subscriber.setServerSocket(System.getProperty("zeroMQAgentInboundSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/zeroMQAgentInboundSocketUrl.pubsub"));
            subscriber.setManyToOne(true);
            subscriber.initialize();
            //Subscribe to all agent messages
            subscriber.subscribe(ZeroMQCommon.AGENT_SUBJECT_PREFIX+"*", zeroMQAgent, String.class);
        } catch (Exception e) {
            logger.error("Error starting ZeroMQAgent .. shutting down.",e);
            if(zeroMQAgent != null)
                zeroMQAgent.dispose();
        }


    }

    void dispose() {
        if(subscriber != null) {
            subscriber.dispose();
        }
        if(publisher != null) {
            publisher.dispose();
        }
        logger.info("ZeroMQAgent .. shutdown.");
    }

    @Override
    public void messageReceived(String subject, Object message) {
        logger.info("subject=["+subject+"] message=["+message+"]");
        publisher.publish(subject, message);
    }
}
