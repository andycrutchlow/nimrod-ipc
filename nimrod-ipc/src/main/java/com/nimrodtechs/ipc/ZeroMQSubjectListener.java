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

/**
 * A runnable program to test whether messages are being received on given subjects.
 * Cannot show actual contents of message payload because the JVM running this does not have access to all
 * the libraries potentially needed to de-serialize.
 *
 * @author andy
 */
public class ZeroMQSubjectListener implements MessageReceiverInterface {
	final static Logger logger = LoggerFactory.getLogger(ZeroMQSubjectListener.class);
	static ZeroMQPubSubSubscriber subscriber;

	public static void main(String[] args) {
		//Register a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			if (subscriber != null)
				subscriber.dispose();
		}));

		String subscriptionSubject = System.getProperty("subject");
		if (subscriptionSubject == null) {
			logger.info("must provide -Dsubject=");
			System.exit(-1);
		}
//        String className = System.getProperty("class");
//        if(className == null) {
//        	logger.info("must provide -Dclass=");
//        	System.exit(-1);
//        }
//        if(className.endsWith(".class")) {
//        	className.replace(".class", "");
//        }
//        Class dataClass = null;
//        try {
//        	dataClass = Class.forName(className);
//        } catch (ClassNotFoundException cnf) {
//        	logger.info(className + " not found on classpath");
//        	System.exit(-1);
//        }
		String publisherSocketUrl = System.getProperty("publisherSocketUrl");
		if (publisherSocketUrl == null) {
			logger.info("must provide -DpublisherSocketUrl=");
			System.exit(-1);
		}

		//Configure the general serializer by adding a kryo serializer
		//NimrodObjectSerializer.GetInstance().getSerializers().put("kryo",new KryoSerializer());
		subscriber = new ZeroMQPubSubSubscriber();
		subscriber.setInstanceName("GeneralSubscriber");
		subscriber.setServerSocket(publisherSocketUrl);
		try {
			subscriber.initialize();
			subscriber.subscribe(subscriptionSubject, new ZeroMQSubjectListener(), byte[].class);
		}
		catch (Exception e) {
			//
			e.printStackTrace();
		}
	}

	@Override
	public void messageReceived(String subject, Object message) {
		//For now message is always a byte[]
		byte[] b = (byte[]) message;
		logger.info("subject=" + subject + " byte[" + b.length + "]");
	}

}
