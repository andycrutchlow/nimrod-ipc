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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nimrodtechs.ipc.queue.QueueExecutor;

public class TestSubscriber implements InstanceEventReceiverInterface  {
    final static Logger logger = LoggerFactory.getLogger(TestSubscriber.class);
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
        // Add an event listener that will be called (if agent is running) when a publisher called 'TestPublisher' is started
        subscriber = new ZeroMQPubSubSubscriber();
        subscriber.addInstanceEventReceiver("TestPublisher", new TestSubscriber());
        subscriber.setInstanceName("TestSubscriber");
        //Attach to the publisher socket
        subscriber.setServerSocket(args[0]);
        //subscriber.setServerSocket(System.getProperty("publisherSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/TestPublisherSocket.pubsub"));
        try {
            subscriber.setUseAgent(true);
            subscriber.initialize();
            //Subscribe to 3 different subjects providing 3 different message handlers
            subscriber.subscribe("testsubject", new TestSubjectHandler(), String.class,QueueExecutor.CONFLATING_QUEUE);
            subscriber.subscribe("testsubject2", new TestSubjectHandler2(), String.class);
            subscriber.subscribe("testsubject3", new TestSubjectHandler3(), TestDTO.class);
            //Subscribe to a wildcard subject e.g. aaaa.bbbb.cccc and aaaa.bbbb.dddd will be routed to it
            subscriber.subscribe("aaaa.bbbb.*", new TestSubjectWildcardHandler(), TestDTO.class);
        } catch (Exception e) {
            //
            e.printStackTrace();
        }
	}

    static class TestSubjectHandler implements MessageReceiverInterface {
        public void messageReceived(String subject, Object message) {
            logger.info("TestSubjectHandler : subject="+subject+" message="+message);
        }
    }
    static class TestSubjectHandler2 implements MessageReceiverInterface {
        public void messageReceived(String subject, Object message) {
            logger.info("TestSubjectHandler2 : subject="+subject+" message="+message);
        }
    }
    static class TestSubjectHandler3 implements MessageReceiverInterface {
        public void messageReceived(String subject, Object message) {
            TestDTO t = (TestDTO)message;
            logger.info("TestSubjectHandler3 : subject="+subject+" field1="+t.field1+" field2="+t.field2+" field3="+t.field3);
        }
    }
    static class TestSubjectWildcardHandler implements MessageReceiverInterface {
        public void messageReceived(String subject, Object message) {
            TestDTO t = (TestDTO)message;
            logger.info("TestSubjectWildcardHandler : subject="+subject+" field1="+t.field1+" field2="+t.field2+" field3="+t.field3);
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
