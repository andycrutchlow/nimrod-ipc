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

public class TestPublisherBManyToOne implements InstanceEventReceiverInterface {
    static ZeroMQPubSubPublisher publisher;
    static TestPublisherBManyToOne instance = new TestPublisherBManyToOne();

    public static void main(String[] args) {
        if(args.length == 0) {
            System.out.println("Provide argument which is describes the common,single Socket that this manyToOne publisher will publish to e.g. tcp://localhost:6062");
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

        // Add an event listener that will be called (if agent is running) when a subscriber called 'TestSubscriberManyToOne' is started or stopped...
        ZeroMQCommon.addInstanceEventReceiver("TestSubscriberManyToOne", instance);

        //Setup the publisher using the socket description passed as an argument
        publisher = new ZeroMQPubSubPublisher();

        try {
            publisher.setManyToOne(true);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        publisher.setServerSocket(args[0]);
        publisher.setInstanceName("TestPublisherB");

        try {
            publisher.setUseAgent(true);
            // Initialize
            publisher.initialize();
            //Loop to send some example messages
            for (int i = 0; i < 1000; i++) {
                //Examples which demonstrates publishing into a single receiver
                TestDTO t2 = new TestDTO();
                t2.setField1("From PublisherB");
                t2.setField2(i);
                t2.setField3(i);
                publisher.publish("md.publisherB.EUR.USD", t2);
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
