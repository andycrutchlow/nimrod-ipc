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

package com.nimrodtechs.demo;

import com.nimrodtechs.ipc.InstanceEventReceiverInterface;
import com.nimrodtechs.ipc.ZeroMQCommon;
import com.nimrodtechs.ipc.ZeroMQPubSubPublisher;

public class TestPublisher implements InstanceEventReceiverInterface {
    static ZeroMQPubSubPublisher publisher;
    static TestPublisher instance = new TestPublisher();

    public static void main(String[] args) {
        if(args.length == 0) {
            System.out.println("Provide argument which is describes the Socket that this publisher will publish on e.g. tcp://localhost:6062");
            System.exit(0);
        }
        // Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if (publisher != null)
                    publisher.dispose();
            }
        });
        // If required configure a serializer. Default is already a kryo serializer
        //NimrodObjectSerializer.GetInstance().getSerializers().put("kryo", new KryoSerializer());

        // Add an event listener that will be called (if agent is running) when a subscriber called 'TestSubscriber' is started
        ZeroMQCommon.addInstanceEventReceiver("TestSubscriber", instance);

        //Setup the publisher using the socket description passed as an argument
        publisher = new ZeroMQPubSubPublisher();
        publisher.setServerSocket(args[0]);

        //Example of alternative socket setup .. using ipc and unix domain socket .. needs native zeromq
        //publisher.setServerSocket(System.getProperty("publisherSocketUrl", "ipc://" + System.getProperty("java.io.tmpdir") + "/TestPublisherSocket.pubsub"));

        publisher.setInstanceName("TestPublisher");

        try {
            publisher.setUseAgent(true);
            // Initialize
            publisher.initialize();
            //Loop to send some example messages
            for (int i = 0; i < 1000; i++) {
                //Publish 3 different messages on 3 different subjects with 3 different payloads
                publisher.publish("testsubject", "testmessage");
                publisher.publish("testsubject2", "testmessage2");
                //Example using a serialized class
            	TestDTO t = new TestDTO();
            	t.setField1("HELLO");
            	t.setField2(i);
            	t.setField3(i);
                publisher.publish("testsubject3", t);

                //Examples which demonstrates wildcard subscription on aaaa.bbbb.* and using a serialized class
                TestDTO t2 = new TestDTO();
                t2.setField1("HELLO2");
                t2.setField2(i);
                t2.setField3(i);
                publisher.publish("aaaa.bbbb.cccc", t2);
                TestDTO t3 = new TestDTO();
                t3.setField1("HELLO3");
                t3.setField2(i);
                t3.setField3(i);
                publisher.publish("aaaa.bbbb.dddd", t3);

                Thread.sleep(2000);
            }
            //Finished
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
