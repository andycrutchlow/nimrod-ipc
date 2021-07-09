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

import com.nimrodtechs.ipc.MessageReceiverInterface;
import com.nimrodtechs.ipc.ZeroMQPubSubSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example of a different threading model to TestSubscriberManyToOne.
 * Messages arriving from publisherA will not affect messages arriving from publisherB and visa versa
 * as they are on their own dispatching queue.
 */
public class TestSubscriberManyToOneAlt {
    final static Logger logger = LoggerFactory.getLogger(TestSubscriberManyToOneAlt.class);
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
        subscriber.setInstanceName("TestSubscriberManyToOne");
        try {
            subscriber.setManyToOne(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //Attach to the publisher socket
        subscriber.setServerSocket(args[0]);
        try {
            subscriber.initialize();
            //Subscribe to wildcard that will receive all messages arriving on subject md.publisherA.*
            subscriber.subscribe("md.publisherA.*", new TestSubjectHandler1(), TestDTO.class);
            //Subscribe to wildcard that will receive all messages arriving on subject md.publisherB.*
            subscriber.subscribe("md.publisherB.*", new TestSubjectHandler2(), TestDTO.class);
        } catch (Exception e) {
            //
            e.printStackTrace();
        }
	}

    static class TestSubjectHandler1 implements MessageReceiverInterface {
        public void messageReceived(String subject, Object message) {
            TestDTO t = (TestDTO)message;
            logger.info("TestSubjectHandler1 : subject="+subject+" field1="+t.field1+" field2="+t.field2+" field3="+t.field3);
        }
    }
    static class TestSubjectHandler2 implements MessageReceiverInterface {
        public void messageReceived(String subject, Object message) {
            TestDTO t = (TestDTO)message;
            logger.info("TestSubjectHandler2 : subject="+subject+" field1="+t.field1+" field2="+t.field2+" field3="+t.field3);
        }
    }

}
