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
import com.nimrodtechs.ipc.queue.QueueExecutor;
import com.nimrodtechs.ipc.queue.SequentialExecutor;
import com.nimrodtechs.serialization.NimrodObjectSerializationInterface;
import com.nimrodtechs.serialization.NimrodObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Add a desc
 *
 * @author nimrod
 */
public abstract class ZeroMQCommon implements MessageReceiverInterface {
	public static final String AGENT_INBOUND_SOCKET = "zeroMQAgentInboundSocketUrl";
	public static final String AGENT_OUTBOUND_SOCKET = "zeroMQAgentOutboundSocketUrl";
	public static final String AGENT_SUBJECT_PREFIX = "nimrod.agent.";
	protected final static String INTERNAL_SOCKET_NAME_PREFIX = "inproc://inproc";
	protected final static String SHUTDOWN_TASKNAME = "shutdowntask";
	protected final static String INTERRUPT_CALLS_IN_PROGRESS_TASKNAME = "interruptcallsinprogresstask";
	protected static final String LOST_CONNECTION = "LostConnectionToServer";
	protected static final String PUMP_PREFIX = "zmqPump-";
	protected static final String HEARTBEAT_PREFIX = "zmqHrtb-";
	protected static final String WORKER_PREFIX = "zmqWorker-";
	protected static final String INPROC_PREFIX = "zmqInproc-";
	protected static final String PUBLISHER_PREFIX = "zmqPub-";
	protected static final String SUBSCRIBER_PREFIX = "zmqSub-";
	protected static final String KEEPALIVE_SUBJECT = "^";
	protected static final char KEEPALIVE_SUBJECT_CHAR = '^';
	protected static final String BROKER_SUBJECT_PREFIX = "0.";
	protected static final String INITIAL_VALUES_SUFFIX = "initialValues";
	protected static final String INSTANCE_SUFFIX = "instance";
	protected static final String IPC_TIME_SUFFIX = "ipctime";
	protected static final byte[] ZERO_AS_BYTES = "0".getBytes();
	protected static final byte[] ONE_AS_BYTES = "1".getBytes();
	protected static final byte[] EMPTY_FRAME = "".getBytes();
	protected final static byte[] SHUTDOWN_OPERATION = "~0".getBytes();
	protected static final byte[] READY_OPERATION = "~1".getBytes();
	protected final static byte[] INTERRUPT_CALLS_IN_PROGRESS_OPERATION = "~2".getBytes();
	protected static final byte[] RESET_OPERATION = "~3".getBytes();
	protected static Context context;
	protected static int TIMEOUT = 2000;
	protected static int RETRY = 3;
	protected static String defaultSerializerId;
	protected static String zeroMQAgentInboundSocketUrl = System.getProperty(AGENT_INBOUND_SOCKET, "ipc://" + System.getProperty("java.io.tmpdir") + "/zeroMQAgentInboundSocketUrl.pubsub");
	protected static String zeroMQAgentOutboundSocketUrl = System.getProperty(AGENT_OUTBOUND_SOCKET, "ipc://" + System.getProperty("java.io.tmpdir") + "/zeroMQAgentOutboundSocketUrl.pubsub");
	protected static ZeroMQPubSubPublisher agentPublisher;
	protected static ZeroMQPubSubSubscriber agentSubscriber;
	private static Logger logger = LoggerFactory.getLogger(ZeroMQCommon.class);
	private static Map<String, ZeroMQCommon> instances = new HashMap<>();
	private static Map<String, List<InstanceEventReceiverInterface>> instanceEventReceivers = new HashMap<>();
	private static List<IpcTimeEventReceiverInterface> ipcTimeEventReceivers = new ArrayList<>();
	protected HashMap<String, byte[]> lastValueCache = new HashMap<>(10000);
	protected List<String> externalSocketURL = new ArrayList<>();
	protected int currentExternalSocketEntry = 0;
	protected boolean alreadyInitialized = false;
	/**
	 * In the context of a rmi client/server then server socket is the listening
	 * side that clients connect to and subsequently send requests in on. In the
	 * context of pubsub then server socket is the socket that messages will be
	 * published on and that subscribers connect to to receive messages on.
	 */
	protected String serverSocket = null;
	protected String serverSocketLongVersion = null;
	protected String clientSocket = null;
	protected Socket frontend;
	protected Socket backend;
	protected NimrodObjectSerializationInterface defaultSerializer;
	protected String instanceName = null;
	/**
	 * manyToOne = true means the socket will be written to by many (this
	 * process is one of them) and read by one (another process).
	 * VERY IMPORTANT : this needs to be set before setting/calling setServerSocket ... this is weak and error prone so needs to be addressed at some point!!
	 */
	protected boolean manyToOne = false;
	protected boolean lastValuePublish = true;
	protected boolean useAgent = false;
	protected boolean alreadyDisposed = false;

	/**
	 * For now limit this number to 1 to 4
	 * @return
	 */
	public static int GetDefaultThreadPoolSize() {
		int size = Runtime.getRuntime().availableProcessors();
		if(size < 2) {
			return 1;
		} else if(size > 4) {
			return 4;
		} else {
			return size / 2;
		}
	}

	public static ZeroMQCommon getInstance(String name) {
		return instances.get(name);
	}

	public static String getZeroMQAgentInboundSocketUrl() {
		return zeroMQAgentInboundSocketUrl;
	}

	public static void setZeroMQAgentInboundSocketUrl(String zeroMQAgentInboundSocketUrl) {
		ZeroMQCommon.zeroMQAgentInboundSocketUrl = zeroMQAgentInboundSocketUrl;
	}

	public static String getZeroMQAgentOutboundSocketUrl() {
		return zeroMQAgentOutboundSocketUrl;
	}

	public static void setZeroMQAgentOutboundSocketUrl(String zeroMQAgentOutboundSocketUrl) {
		ZeroMQCommon.zeroMQAgentOutboundSocketUrl = zeroMQAgentOutboundSocketUrl;
	}

	public static void PublishIpcTimeEvent(long time) {
		if (agentPublisher != null) {
			long offset = time - System.currentTimeMillis();
			agentPublisher.publish(AGENT_SUBJECT_PREFIX + IPC_TIME_SUFFIX, Long.toString(offset));
//    		try {
//				Thread.sleep(5000);
//			} catch (InterruptedException e) {
//			}
		}
	}

	public static void addInstanceEventReceiver(String instanceName, InstanceEventReceiverInterface listener) {
		List<InstanceEventReceiverInterface> listeners = instanceEventReceivers.computeIfAbsent(instanceName, k -> new ArrayList<>());
		if (listeners.contains(listener) == false) {
			listeners.add(listener);
		}
	}

	public static void addIpcTimeEventReceiver(IpcTimeEventReceiverInterface listener) {
		if (ipcTimeEventReceivers.contains(listener) == false) {
			ipcTimeEventReceivers.add(listener);
		}
	}

	public static void removeIpcTimeEventReceiver(IpcTimeEventReceiverInterface listener) {
		if (ipcTimeEventReceivers.contains(listener) == true) {
			ipcTimeEventReceivers.remove(listener);
		}
	}

	static byte[] convertLongToBytes(long l) {
		byte[] message = new byte[8];
		for (int i = 7; i > 0; i--) {
			message[i] = (byte) l;
			l >>>= 8;
		}
		message[0] = (byte) l;
		return message;
	}

	static long convertBytesToLong(byte[] by) {
		long value = 0;
		for (byte aBy : by) {
			value = (value << 8) + (aBy & 0xff);
		}
		return value;
	}

	public String getInstanceName() {
		if (instanceName == null)
			instanceName = getDefaultInstanceName();
		return instanceName;
	}

	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
		instances.put(instanceName, this);
	}

	protected abstract String getDefaultInstanceName();

	public void setManyToOne(boolean manyToOne) throws Exception {
		if (this instanceof ZeroMQPubSubSubscriber == false && this instanceof ZeroMQPubSubPublisher == false)
			throw new NimrodPubSubException("setManyToOne only applicable for ZeroMQPubSubSubscriber or ZeroMQPubSubPublisher");
		if(manyToOne == true && serverSocket != null) {
			throw new NimrodPubSubException("setManyToOne true must be done before setting socket");
		}
		this.manyToOne = manyToOne;
	}

	public boolean isLastValuePublish() {
		return lastValuePublish;
	}

	public void setLastValuePublish(boolean lastValuePublish) {
		this.lastValuePublish = lastValuePublish;
	}

	public void setUseAgent(boolean useAgent) {
		this.useAgent = useAgent;
	}

	public boolean initialize() throws Exception {
		if (alreadyInitialized)
			return false;
		// If externalSocketURL's have not already been injected then see if
		// available on command line
		if (serverSocket == null) {
			String sockName = System.getProperty("nimrod.rpc.serverRmiSocket");
			if (sockName == null) {
				throw new Exception("Missing serverRmiSocket configuration");
			}
			else {
				setServerSocket(sockName);
			}

		}

//        if (NimrodObjectSerializer.GetInstance().getSerializers().size() == 0)
//            NimrodObjectSerializer.GetInstance().getSerializers().put(NimrodObjectSerializer.DEFAULT_SERIALIZATION_ID, new KryoSerializer());

		// TODO This needs a bit more work..if there are multiples then need to
		// pass in which is default
		for (Map.Entry<String, NimrodObjectSerializationInterface> entry : NimrodObjectSerializer.GetInstance().getSerializers().entrySet()) {
			defaultSerializer = entry.getValue();
			defaultSerializerId = entry.getKey();
			break;
		}
		if (defaultSerializer == null) {
			throw new Exception("No NimrodObjectSerializers available. Atleast 1 must be configured.");
		}
		if (context != null) {
			logger.info("ZMQ context already initialized Version : " + ZMQ.getFullVersion());
		}
		else {
			context = ZMQ.context(1);
			logger.info("created ZMQ context Version : " + ZMQ.getFullVersion());
		}
		alreadyInitialized = true;
		return true;
	}

	public String getServerSocket() {
		return serverSocket;
	}

	public void setServerSocket(String sockName) {
		if (sockName != null) {
			clientSocket = sockName;
			if (sockName.startsWith("tcp://") && ((this instanceof ZeroMQRmiServer) || (this instanceof ZeroMQPubSubPublisher && manyToOne == false) || (this instanceof ZeroMQPubSubSubscriber && manyToOne == true))) {
				String s1 = sockName.replace("tcp://", "");
				String[] s2 = s1.split(":");
				if (s2.length > 1) {
					if (s2[0].equals("*")) {
						try {
							serverSocketLongVersion = "tcp://" + InetAddress.getLocalHost().getHostName() + ":" + s2[1];
							serverSocket = sockName;
						}
						catch (UnknownHostException e) {
							this.serverSocket = sockName;
						}
					}
					else {
						serverSocketLongVersion = sockName;
						serverSocket = "tcp://*:" + s2[1];
					}
				}
				else {
					this.serverSocket = sockName;
				}
			}
			else {
				this.serverSocket = sockName;
			}
			if (serverSocketLongVersion == null)
				serverSocketLongVersion = serverSocket;
		}
	}

	/**
	 * A method to check equality of first 2 bytes. Good enough to recognize all
	 * of the internal activities.
	 *
	 * @param lhs
	 * @param rhs
	 * @return
	 */
	protected boolean testHighOrderBytesAreEqual(byte[] lhs, byte[] rhs) {
		if (lhs.length < 2 || rhs.length < 2)
			return false;
		if (lhs[0] == rhs[0] && lhs[1] == rhs[1])
			return true;
		else
			return false;
	}

	protected void initializeAgent() {
		// Only initialize agent stuff once
		if ((agentSubscriber != null || agentPublisher != null) && this instanceof ZeroMQRmiServer == false)
			return;
		if (instanceName.equals("agentSubscriber") == false && instanceName.equals("agentPublisher") == false && getZeroMQAgentOutboundSocketUrl() != null && getZeroMQAgentInboundSocketUrl() != null) {
			//if (this instanceof ZeroMQPubSubSubscriber == false) {
				initializeAgentSubscriber();
			//}
			initializeAgentPublisher();
			agentPublish(AGENT_SUBJECT_PREFIX + INSTANCE_SUFFIX, instanceName + "," + getServerSocket() + "," + InstanceEventReceiverInterface.INSTANCE_UP);
		}
	}

	public void initializeAgentSubscriber() {
		if (agentSubscriber != null)
			return;
		agentSubscriber = new ZeroMQPubSubSubscriber();
		agentSubscriber.setServerSocket(getZeroMQAgentOutboundSocketUrl());
		agentSubscriber.setInstanceName("agentSubscriber");
		try {
			agentSubscriber.initialize();
			QueueExecutor sequentialExecutor = new SequentialExecutor();
			sequentialExecutor.setThreadNamePrefix(agentSubscriber.getInstanceName());
			sequentialExecutor.setThreadPoolSize(1);
			sequentialExecutor.initialize();
			agentSubscriber.setSequentialExecutor(sequentialExecutor);
			agentSubscriber.subscribe(ZeroMQCommon.AGENT_SUBJECT_PREFIX + INSTANCE_SUFFIX, this, String.class);
			agentSubscriber.subscribe(ZeroMQCommon.AGENT_SUBJECT_PREFIX + IPC_TIME_SUFFIX, this, String.class);
		}
		catch (Exception e) {
			logger.error("initializeAgentOutboundSocket : failed", e);
		}
	}

	protected void initializeAgentPublisher() {
		if (agentPublisher != null) {
			return;
		}
		agentPublisher = new ZeroMQPubSubPublisher();
		try {
			agentPublisher.setManyToOne(true);
			agentPublisher.setServerSocket(getZeroMQAgentInboundSocketUrl());
			agentPublisher.setInstanceName("agentPublisher");
			agentPublisher.initialize();
		}
		catch (Exception e) {
			logger.error("initializeAgentInboundSocket : failed", e);
		}
	}

	protected void dispose() {
		if (getInstanceName() != null && (getInstanceName().equals("agentSubscriber") || getInstanceName().equals("agentPublisher")))
			return;
		agentPublish(AGENT_SUBJECT_PREFIX + INSTANCE_SUFFIX, instanceName + "," + getServerSocket() + "," + InstanceEventReceiverInterface.INSTANCE_DOWN);
		if (agentSubscriber != null) {
			agentSubscriber.dispose();
			agentSubscriber = null;
		}
		if (agentPublisher != null) {
			agentPublisher.dispose();
			agentPublisher = null;
		}
	}

	protected void agentPublish(String subject, String message) {
		if (agentPublisher != null)
			agentPublisher.publish(subject, message);
	}

	@Override
	public void messageReceived(String subject, Object message) {
		if (subject.equals(AGENT_SUBJECT_PREFIX + INITIAL_VALUES_SUFFIX)) {
			String initialValueSubject = (String) message;
			byte[] initialValue = lastValueCache.get(initialValueSubject);
			if (initialValue != null) {
				((ZeroMQPubSubPublisher) this).publish(initialValueSubject, initialValue);
				logger.info("Published initial values for subject=[" + message + "]");
			}
		}
		else if (subject.equals(AGENT_SUBJECT_PREFIX + INSTANCE_SUFFIX)) {
			// Notity listeners for this event
			String[] parts = ((String) message).split(",");
			List<InstanceEventReceiverInterface> listeners = instanceEventReceivers.get(parts[0]);
			if (listeners == null)
				return;
			int instanceStatus = Integer.parseInt(parts[2]);
			for (InstanceEventReceiverInterface listener : listeners) {
				listener.instanceEvent(parts[0], parts[1], instanceStatus);
			}
		}
		else if (subject.equals(AGENT_SUBJECT_PREFIX + IPC_TIME_SUFFIX)) {
			// Notity listeners for this event
			long offset = Long.parseLong((String) message);
			for (IpcTimeEventReceiverInterface ipcTimeEventReceiver : ipcTimeEventReceivers) {
				ipcTimeEventReceiver.ipcTimeEvent(offset);
			}
		}
	}
}
