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

package com.nimrodtechs.test;

import com.nimrodtechs.ipc.InstanceEventReceiverInterface;
import com.nimrodtechs.ipc.ZeroMQCommon;
import com.nimrodtechs.ipc.ZeroMQPubSubPublisher;

public class TestPublisher implements InstanceEventReceiverInterface {
    static ZeroMQPubSubPublisher publisher;
    static TestPublisher instance = new TestPublisher();

    public static void main(String[] args) {
        // Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if (publisher != null)
                    publisher.dispose();
            }
        });
        // Configure the general serializer by adding a kryo serializer
        //NimrodObjectSerializer.GetInstance().getSerializers().put("kryo", new KryoSerializer());
        // Add event listener that will be called (if agent is running) when subscriber 'TestSubscriber' is started
        ZeroMQCommon.addInstanceEventReceiver("TestSubscriber", instance);
        publisher = new ZeroMQPubSubPublisher();
        publisher.setServerSocket(System.getProperty("publisherSocketUrl", "ipc://" + System.getProperty("java.io.tmpdir") + "/TestPublisherSocket.pubsub"));
        publisher.setInstanceName("testpublisher");
        try {
            // Initialize
            publisher.initialize();
            for (int i = 0; i < 1000; i++) {
                publisher.publish("testsubject", "testmessage");
                publisher.publish("testsubject2", "testmessage2");
            	TestDTO t = new TestDTO();
            	t.setField1("HELLO");
            	t.setField2(i);
            	t.setField3(i);
                publisher.publish("testsubject3", t);
                Thread.sleep(2000);
            }
            publisher.dispose();

        } catch (Exception e) {
            //
            e.printStackTrace();
        }

    }

    @Override
    public void instanceEvent(String instanceName, String instanceUrl, int instanceStatus) {
        if(instanceStatus == 0)
            System.out.println("instanceName=["+instanceName+"] instanceUrl=["+instanceUrl+"] status=["+instanceStatus+"] is now running");
        else if(instanceStatus == 1)
            System.out.println("instanceName=["+instanceName+"] instanceUrl=["+instanceUrl+"] status=["+instanceStatus+"] has stopped");
    }
}
