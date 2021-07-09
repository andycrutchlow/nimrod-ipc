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

import com.nimrodtechs.ipc.ZeroMQPubSubPublisherViaBroker;

public class TestBrokerPublisher  {
    static ZeroMQPubSubPublisherViaBroker publisher;
    static TestBrokerPublisher instance = new TestBrokerPublisher();

    public static void main(String[] args) {
        // Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if (publisher != null)
                    publisher.dispose();
            }
        });
        publisher = new ZeroMQPubSubPublisherViaBroker();
        publisher.setServerSocket(System.getProperty("zeroMQBrokerInboundSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/zeroMQBrokerInboundSocketUrl.pubsub"));
        publisher.setInstanceName("testpublisher");
        try {
            // Initialize
            publisher.initialize();
            for (int i = 0; i < 1000; i++) {
            	TestDTO t = new TestDTO();
            	t.setField1("HELLO");
            	t.setField2(i);
            	t.setField3(i);
                publisher.publish("testsubject3", t);
                Thread.sleep(2000);
            }
            publisher.dispose();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
