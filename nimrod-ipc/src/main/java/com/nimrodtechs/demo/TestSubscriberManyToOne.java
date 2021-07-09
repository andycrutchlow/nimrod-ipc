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
import com.nimrodtechs.ipc.MessageReceiverInterface;
import com.nimrodtechs.ipc.ZeroMQCommon;
import com.nimrodtechs.ipc.ZeroMQPubSubSubscriber;
import com.nimrodtechs.ipc.queue.QueueExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSubscriberManyToOne implements InstanceEventReceiverInterface {
    final static Logger logger = LoggerFactory.getLogger(TestSubscriberManyToOne.class);
    static TestSubscriberManyToOne instance = new TestSubscriberManyToOne();
    static ZeroMQPubSubSubscriber subscriber;
    
	public static void main(String[] args) {
        if(args.length == 0) {
            System.out.println("Provide argument which points to the Socket that the publisher is publishing on e.g. tcp://localhost:6062");
            System.exit(0);
        }
	  //Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if(subscriber != null)
                    subscriber.dispose();
            }
        });

        //Configure the general serializer by adding a kryo serializer
        //NimrodObjectSerializer.GetInstance().getSerializers().put("kryo",new KryoSerializer());
        subscriber = new ZeroMQPubSubSubscriber();
        // Add an event listener that will be called (if agent is running) when a subscriber called 'TestSubscriberManyToOne' is started or stopped...
        subscriber.addInstanceEventReceiver("TestPublisherA", instance);
        subscriber.addInstanceEventReceiver("TestPublisherB", instance);

        subscriber.setInstanceName("TestSubscriberManyToOne");
        try {
            subscriber.setManyToOne(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //Attach to the publisher socket
        subscriber.setServerSocket(args[0]);
        try {
            subscriber.setUseAgent(true);
            subscriber.initialize();
            //Subscribe to wildcard that will receive all messages arriving on subject md.*
            subscriber.subscribe("md.*", new TestSubjectHandler(), TestDTO.class);
        } catch (Exception e) {
            //
            e.printStackTrace();
        }
	}

    static class TestSubjectHandler implements MessageReceiverInterface {
        public void messageReceived(String subject, Object message) {
            TestDTO t = (TestDTO)message;
            logger.info("TestSubjectHandler : subject="+subject+" field1="+t.field1+" field2="+t.field2+" field3="+t.field3);
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
