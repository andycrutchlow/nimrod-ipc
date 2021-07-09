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
 * Optional broker process that can be run to provide a brokered centralized distribution of published messages.
 * The main difference is that subscriber processes can subscribe to one place to receive all possible published messages rather than
 * having to know the respective process to subject relationship. Downside is now there is always 2 sockets to traverse from producer to
 * consumer and multiple sources can be publishing on the same subject and the consumer will just see as one stream...which maybe is what is wanted.
 * Instantiates itself as a many-to-one subscriber and a one-to-many publisher.
 * At least two sockets are required for this operation. They are domain specific and need to be on
 * 'well-known' locations that all other processes that want to participate will know about.
 * i.e. the participants are configured with these well known values.
 *
 * @author andy
 */
public class ZeroMQBroker implements MessageReceiverInterface {
	static ZeroMQPubSubSubscriber subscriber;
	static ZeroMQPubSubPublisher publisher;
	private static Logger logger = LoggerFactory.getLogger(ZeroMQBroker.class);
	private static ZeroMQBroker zeroMQBroker = null;

	public static void main(String[] args) {
		//Register a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			if (zeroMQBroker != null)
				zeroMQBroker.dispose();
		}));

		//Configure the general serializer by adding a kryo serializer
		//NimrodObjectSerializer.GetInstance().getSerializers().put(NimrodObjectSerializer.DEFAULT_SERIALIZATION_ID,new KryoSerializer());
		try {
			zeroMQBroker = new ZeroMQBroker();

			publisher = new ZeroMQPubSubPublisher();
			publisher.setServerSocket(System.getProperty("zeroMQBrokerOutboundSocketUrl", "ipc://" + System.getProperty("java.io.tmpdir") + "/zeroMQBrokerOutboundSocketUrl.pubsub"));
			publisher.setInstanceName("brokerPublisher");
			publisher.initialize();

			subscriber = new ZeroMQPubSubSubscriber();
			subscriber.setInstanceName("brokerSubscriber");
			subscriber.setManyToOne(true);
			subscriber.setServerSocket(System.getProperty("zeroMQBrokerInboundSocketUrl", "ipc://" + System.getProperty("java.io.tmpdir") + "/zeroMQBrokerInboundSocketUrl.pubsub"));
			subscriber.initialize();
			//Subscribe to all broker messages
			subscriber.subscribe(ZeroMQCommon.BROKER_SUBJECT_PREFIX + "*", zeroMQBroker, byte[].class);
		}
		catch (Exception e) {
			logger.error("Error starting ZeroMQBroker .. shutting down.", e);
			if (zeroMQBroker != null)
				zeroMQBroker.dispose();
		}


	}

	void dispose() {
		if (subscriber != null) {
			subscriber.dispose();
		}
		if (publisher != null) {
			publisher.dispose();
		}
		logger.info("ZeroMQBroker .. shutdown.");
	}

	@Override
	public void messageReceived(String subject, Object message) {
		//logger.info("subject=["+subject+"] message=["+message+"]");
		publisher.publish(subject.substring(2), message);
	}
}
