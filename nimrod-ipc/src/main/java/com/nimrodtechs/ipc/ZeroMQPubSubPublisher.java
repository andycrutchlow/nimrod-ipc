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

import com.nimrodtechs.exceptions.NimrodPubSubException;
import com.nimrodtechs.exceptions.NimrodSerializationException;
import com.nimrodtechs.exceptions.NimrodSerializerNotFoundException;
import com.nimrodtechs.serialization.NimrodObjectSerializationInterface;
import com.nimrodtechs.serialization.NimrodObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO ... need a periodic publish to a keepalive message to stop established sockets being dropped.
 *
 * @author andy
 */
public class ZeroMQPubSubPublisher extends ZeroMQCommon {
	final static Logger logger = LoggerFactory.getLogger(ZeroMQPubSubPublisher.class);
	static List<byte[]> STOP = new ArrayList<>();
	private static AtomicInteger instanceId = new AtomicInteger(0);
	/**
	 * If we ever get 2048 messages behind then we are in big trouble...
	 */
	private static int queueSize = 2048;
	private static int alertLevel = queueSize / 10;
	private final BlockingQueue<List<byte[]>> queue = new ArrayBlockingQueue<>(queueSize);
	private int thisInstanceId = instanceId.getAndIncrement();
	private ReentrantLock setupLock = new ReentrantLock();
	private Condition setupCondition = setupLock.newCondition();
	private Thread publisherThread;
	private Timer timer;
	private int keepAliveInterval = 60000;
	private boolean keepRunning = true;

	class KeepAliveTask extends TimerTask {
		@Override
		public void run() {
			publish(defaultSerializerId, KEEPALIVE_SUBJECT, "keepalive", false);
		}
	}

	class PublisherThreadHandler implements Runnable {

		@Override
		public void run() {
			setupLock.lock();
			logger.info("PublisherThreadHandler starting..");
			ZMQ.Socket socket = context.socket(ZMQ.XPUB);


			socket.setRcvHWM(0);
			logger.info("Receive HwM for publisher socket {} is {}", getInstanceName(), socket.getRcvHWM());

			// Decide whether to bind or connect
			if (manyToOne) {
				logger.info("manyToOne so connect to " + getServerSocket());
				socket.connect(getServerSocket());
			}
			else {
				logger.info("oneToMany so bind to " + getServerSocket());
				socket.bind(getServerSocket());
			}

			ZMQ.Poller poller = context.poller(1);

			poller.register(socket);

			setupCondition.signal();
			setupLock.unlock();
			logger.info("PublisherThreadHandler ready");
			while (keepRunning) {
				// At a minimum we only dump any requests every 5 seconds
				poller.poll(0);
				if ( poller.pollin(0)) {
					do {
						byte[] message = socket.recv(0);
						logger.info("Received zeromq pub message [{}]", bytesToHex(message));
					} while (socket.hasReceiveMore());
				}
				try {
					List<byte[]> message = queue.poll(5L, TimeUnit.SECONDS);
					if  ( message != null ) {
						if (message == STOP) {
							//This is a signal to stop
							keepRunning = false;
							continue;
						}
						// First entry in message list is subject for message
						// publish
						if (message.size() == 0)
							// Cannot process empty entries
							continue;
						// Look for an explicit stop..first byte in first entry set
						// to zero...which won't clash with usual subject bytes
						if (message.get(0)[0] == 0) {
							keepRunning = false;
							continue;
						}
						// Send the subject
						socket.send(message.get(0), ZMQ.SNDMORE);
						// Send the first message..there is always one atleast
						socket.send(message.get(1), 0);
					}
				}
				catch (InterruptedException e) {
					keepRunning = false;
				}
			}
			socket.close();
			if (setupLock.isHeldByCurrentThread())
				setupLock.unlock();
			logger.info(" PublisherThreadHandler shutting down");
		}
	}

	private final String bytesToHex(byte[] bytes) {
		byte[] slice = Arrays.copyOfRange(bytes, 1, bytes.length);
		return (bytes[0] == 1 ? "SUBSCRIBE TO : " : "UNSUBSCRIBE FROM : ")  +new String(slice);
	}


	public static void publishOnInstance(String instanceName, String subject, Object message) throws NimrodPubSubException {
		publishOnInstance(instanceName, defaultSerializerId, subject, message);
	}

	public static void publishOnInstance(String instanceName, String serializationFormatId, String subject, Object message) throws NimrodPubSubException {
		//Lookup the instance based on name
		ZeroMQPubSubPublisher publisher = (ZeroMQPubSubPublisher) getInstance(instanceName);
		if (publisher == null)
			throw new NimrodPubSubException();
	}

	public void setKeepAliveInterval(int keepAliveInterval) {
		this.keepAliveInterval = keepAliveInterval;
	}

	@Override
	protected String getDefaultInstanceName() {
		return "zmqPublisher";
	}

	public boolean initialize() throws Exception {
		if (super.initialize() == false)
			return false;

		// Start a dedicated thread to manage the publishing
		try {
			PublisherThreadHandler publisherHandler = new PublisherThreadHandler();

			String threadName = PUBLISHER_PREFIX + getInstanceName() + "-" + thisInstanceId;
			publisherThread = new Thread(publisherHandler, threadName);
			setupLock.lock();
			publisherThread.start();
			setupCondition.await();
			Thread.sleep(100);
			if (lastValuePublish && useAgent) {
				initializeAgent();
			}
			if (getInstanceName() != null && getInstanceName().equals("agentPublisher") == false && getInstanceName().equals("agentSubscriber") == false && agentSubscriber != null) {
				agentSubscriber.subscribe(AGENT_SUBJECT_PREFIX + INITIAL_VALUES_SUFFIX, this, String.class);
			}
			if (serverSocket != null && serverSocket.startsWith("tcp")) {
				timer = new Timer(getInstanceName() + "-" + thisInstanceId + "-keepAliveTimer");
				timer.schedule(new KeepAliveTask(), keepAliveInterval, keepAliveInterval);
			}
			logger.info(getInstanceName() + " initialized.");
		}
		catch (InterruptedException ie) {

		}
		finally {
			if (setupLock.isHeldByCurrentThread())
				setupLock.unlock();
		}
		return true;
	}

	public void dispose() {
		if (alreadyDisposed) {
			logger.info(getInstanceName() + " alreadyDisposed");
			return;
		}
		alreadyDisposed = true;
		super.dispose();
		if (queue.remainingCapacity() < queueSize) {
			try {
				// Give it a chance to close and flush before returning..
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
			}
		}
		keepRunning = false;
		if (timer != null)
			timer.cancel();
		try {
			queue.put(STOP);
		}
		catch (InterruptedException e) {
			logger.info("queue.put(stopWrapper) execption : ", e);
		}
		publisherThread.interrupt();
	}

	public void publish(String subject, Object message) {
		publish(defaultSerializerId, subject, message, true);
	}

	public void publish(String subject, Object message, boolean saveLatestValue) {
		publish(defaultSerializerId, subject, message, saveLatestValue);
	}

	public void publish(String serializationFormatId, String subject, Object message, boolean saveLatestValue) {
		try {
			NimrodObjectSerializationInterface serializer = NimrodObjectSerializer.GetInstance().getSerializers().get(serializationFormatId);
			if (serializer == null) {
				throw new NimrodSerializerNotFoundException();
			}
			byte[] messageAsBytes;
			boolean isByteArray = false;
			//No classDictionary configured so just serialize actual payload object
			if (message instanceof byte[] == false)
				messageAsBytes = serializer.serialize(message);
			else {
				isByteArray = true;
				messageAsBytes = (byte[]) message;
			}
			//Insert timestamp at start of message..if the original message is not a byte[] ... assume timestamp already there is its a byte[]
			byte[] messageAsBytesWithTimestamp;
			if (isByteArray)
				messageAsBytesWithTimestamp = messageAsBytes;
			else
				messageAsBytesWithTimestamp = insertLong(messageAsBytes, System.nanoTime());
			publishRaw(subject, messageAsBytesWithTimestamp);
			//Don't store initial value publish messages
			if (subject.startsWith(AGENT_SUBJECT_PREFIX) == false && saveLatestValue == true)
				lastValueCache.put(subject, messageAsBytesWithTimestamp);

		}
		catch (NimrodSerializationException e) {
			logger.error("Error serializing data, cannot publish data", e);
		}

	}

	/**
	 * More efficient way (8X) of inserting long (e.g. a timestamp,8 bytes) before an existing byte array
	 *
	 * @param aMessage
	 * @return
	 */
	private byte[] insertLong(byte[] aMessage, long time) {
		byte[] revisedMessage = new byte[aMessage.length + 8];
		for (int i = 7; i > 0; i--) {
			revisedMessage[i] = (byte) time;
			time >>>= 8;
		}
		revisedMessage[0] = (byte) time;
		System.arraycopy(aMessage, 0, revisedMessage, 8, aMessage.length);
		return revisedMessage;
	}

	private void publishRaw(String subject, byte[] message) {
		List<byte[]> fullMessage = new ArrayList<>();
		fullMessage.add(subject.getBytes());
		fullMessage.add(message);
//        if ( queue.remainingCapacity() < alertLevel )
//        {
//            // Alert that we are at 90% queue filled
//            logger.warn("Instance "+getInstanceName()+" at 90% queue filled :"+queue.remainingCapacity()+" left");
//        }
//        if ( queue.remainingCapacity() < 2 )
//        {
//            // Alert that we are at 90% queue filled
//            logger.warn("Instance "+getInstanceName()+" has "+queue.remainingCapacity()+" left");
//        }
		try {
			queue.put(fullMessage);
		}
		catch (InterruptedException e) {
			logger.error("Interupted whilst publish", e);
		}
	}

}
