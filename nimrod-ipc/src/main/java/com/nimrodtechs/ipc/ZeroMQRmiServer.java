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

import com.nimrodtechs.annotations.ExposedMethod;
import com.nimrodtechs.annotations.ExposedServiceName;
import com.nimrodtechs.exceptions.NimrodRmiNotConnectedException;
import com.nimrodtechs.serialization.NimrodObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ZeroMQRmiServer extends ZeroMQCommon {
	final static Logger logger = LoggerFactory.getLogger(ZeroMQRmiServer.class);
	private static AtomicInteger instanceId = new AtomicInteger(0);
	private static Thread queueHandlerThread;
	Class parameterTypes[] = new Class[]{List.class};
	private boolean enabled = false;
	private int thisInstanceId = instanceId.getAndIncrement();

	private String internalSocketName;
	/**
	 * This is assignable or will default to available processors / 2.
	 */
	private int workerThreadPoolInitialSize = -1;
	private String workerPrefix;
	private QueueHandlerThread queueHandler;
	/**
	 * Status has 4 possible values, 0 = not ready, 1 = ready for work, 2 =
	 * busy, 3 = shutdown doing some work. The lifecycle for a worker will be
	 * 0->1->2->1->2->1 ..etc...finally ->3 when set to shutdown.
	 */
	private int[] workerStatus;
	private int workerSafetyMargin;
	private ReentrantLock workerThreadLock = new ReentrantLock();
	private Condition workerThreadCondition = workerThreadLock.newCondition();
	/**
	 * Provide ability to register services and specific methods directly
	 * accessible to worker threads.
	 */
	private HashMap<String, ServiceMethods> exposedServices = new HashMap<>();

	class ServiceMethods {
		String serviceName;
		Object service;
		private HashMap<String, MethodWrapper> methods = new HashMap<>();
	}

	class MethodWrapper {
		Method method;
		AtomicInteger callCount = new AtomicInteger(0);
		AtomicLong cummulativeTime = new AtomicLong(0);
	}

	class QueueHandlerThread implements Runnable {
		boolean keepRunning = true;
		boolean inShutDownMode = false;
		ReentrantLock lock = new ReentrantLock();

		void stop() {
			// Connect to the internal queue and send it a shutdown message
			Socket socket = context.socket(ZMQ.REQ);
			socket.setIdentity(SHUTDOWN_TASKNAME.getBytes());
			socket.connect(internalSocketName);
			socket.send(SHUTDOWN_OPERATION, 0);
		}

		@Override
		public void run() {
			lock.lock();
			logger.info("serverside" + thisInstanceId + " rmi starting..");
			// The external interface/socket into this process
			try {
				frontend = context.socket(ZMQ.ROUTER);
				frontend.bind(getServerSocket());
				logger.info(thisInstanceId+" listening on socket "+getServerSocket());
			}
			catch (Exception e) {
				logger.error("serverside" + thisInstanceId + " rmi unable to initialise using " + getServerSocket(), e);
				// This is a serious error .. need to propagate back to main
				// server
				frontend = null;
				lock.unlock();
				return;
			}

			Socket backend = context.socket(ZMQ.ROUTER);
			// This is fixed and private to this process
			backend.bind(internalSocketName);

			// Initialize poll set
			//TODO replace with : Poller items = new ZMQ.Poller(2);
			Poller items = context.poller(2);
			// Always poll for client activity on frontend
			items.register(frontend, Poller.POLLIN);
			// Always poll for worker thread responses on backend
			items.register(backend, Poller.POLLIN);
			logger.info("serverside" + thisInstanceId + " rmi ready");
			lock.unlock();
			while (!Thread.currentThread().isInterrupted() && keepRunning) {
				items.poll();
				// Handle client activity on frontend..forward on to dealer
				if (items.pollin(0)) {
					//logger.info("items.pollin(0)");
					try {
						byte[] returnAddressPart1;
						byte[] returnAddressPart2 = null;
						byte[] empty = null;
						byte[] threadRequestIdFrame = null;
						byte[] firstDataFrame = null;

						// Add the existing return multipart address
						returnAddressPart1 = frontend.recv(0);
						if (frontend.hasReceiveMore())
							returnAddressPart2 = frontend.recv(0);

						// The should be empty
						if (frontend.hasReceiveMore())
							empty = frontend.recv(0);

						// Add the actual data parts..needs to be a loop
						if (frontend.hasReceiveMore())
							threadRequestIdFrame = frontend.recv(0);

						// Add the actual data parts..needs to be a loop
						if (frontend.hasReceiveMore())
							firstDataFrame = frontend.recv(0);

						//See if this is a HEARTBEAT message
						if (threadRequestIdFrame != null && threadRequestIdFrame.length > 0 && new String(threadRequestIdFrame).startsWith(HEARTBEAT_PREFIX)) {
							// Do not respond to heartbeats until overall
							// service is switched on
							if (isEnabled() == false) {
								logger.info("Heartbeat received..but service not enabled yet");
								continue;
							}
							// Its a heartbeat from a client and this process is ready so return immediately
							frontend.send(returnAddressPart1, ZMQ.SNDMORE);
							frontend.send(returnAddressPart2, ZMQ.SNDMORE);
							frontend.send(EMPTY_FRAME, ZMQ.SNDMORE);
							// Prefix with threadRequestIdFrame..i.e. a
							// heartbeat thread
							frontend.send(threadRequestIdFrame, ZMQ.SNDMORE);
							// Prefix with 1 to indicate no error/exception
							frontend.send(ONE_AS_BYTES, ZMQ.SNDMORE);
							// Echo back whatever was sent
							frontend.send(firstDataFrame, 0);
							continue;
						}

						if (inShutDownMode) {
							frontend.send(returnAddressPart1, ZMQ.SNDMORE);
							frontend.send(returnAddressPart2, ZMQ.SNDMORE);
							frontend.send(EMPTY_FRAME, ZMQ.SNDMORE);
							frontend.send(threadRequestIdFrame, ZMQ.SNDMORE);
							frontend.send(ZERO_AS_BYTES, ZMQ.SNDMORE);
							// For now just pass back exception name...add more
							// sophistication later...
							frontend.send(NimrodRmiNotConnectedException.class.getName().getBytes(), 0);
							continue;
						}

						// Get first available worker
						int workerId = getFreeWorker();

						String workAddr = workerPrefix + (workerId);

						// At this point if there is no worker thread
						// available..either because they are all busy OR
						// because we are in the process of shutting down ..so reply
						// immediately back to caller with error
						if (workerId == -1) {
							logger.info("Run out of workerThreads - reject RMI call");
							frontend.send(returnAddressPart1, ZMQ.SNDMORE);
							frontend.send(returnAddressPart2, ZMQ.SNDMORE);
							frontend.send(EMPTY_FRAME, ZMQ.SNDMORE);
							frontend.send(threadRequestIdFrame, ZMQ.SNDMORE);
							frontend.send(ZERO_AS_BYTES, ZMQ.SNDMORE);
							// For now just pass back exception name...add more
							// sophistication later...
							frontend.send(NimrodRmiNotConnectedException.class.getName().getBytes(), 0);
							continue;
						}

						if (threadRequestIdFrame == null || firstDataFrame == null) {
							logger.error("threadRequestIdFrame == null,returnAddressPart1=[" + (returnAddressPart1 != null ? new String(returnAddressPart1) : "null") + "]" + " returnAddressPart2=[" + (returnAddressPart2 != null ? new String(returnAddressPart2) : "null") + "]");
							if (returnAddressPart1 != null && returnAddressPart2 != null) {
								frontend.send(returnAddressPart1, ZMQ.SNDMORE);
								frontend.send(returnAddressPart2, ZMQ.SNDMORE);
								frontend.send(EMPTY_FRAME, ZMQ.SNDMORE);
								frontend.send(threadRequestIdFrame != null ? threadRequestIdFrame : "UNKNOWN".getBytes(), ZMQ.SNDMORE);
								frontend.send(ZERO_AS_BYTES, ZMQ.SNDMORE);
								// For now just pass back exception name...add more sophistication later...
								frontend.send(NimrodRmiNotConnectedException.class.getName().getBytes(), 0);
							}
							continue;
						}
						// At this point we are forwarding on a good inbound
						// request to a worker thread for processing.
						// Add/Prefix the destination worker threads address
						backend.send(workAddr.getBytes(), ZMQ.SNDMORE);
						backend.send(EMPTY_FRAME, ZMQ.SNDMORE);
						safeZmqSend(backend, returnAddressPart1, ZMQ.SNDMORE);
						safeZmqSend(backend, returnAddressPart2, ZMQ.SNDMORE);
						backend.send(EMPTY_FRAME, ZMQ.SNDMORE);
						safeZmqSend(backend, threadRequestIdFrame, ZMQ.SNDMORE);
						if (frontend.hasReceiveMore()) {
							safeZmqSend(backend, firstDataFrame, ZMQ.SNDMORE);
							boolean more = true;
							while (more) {
								byte[] nextFrame = frontend.recv(0);

								more = frontend.hasReceiveMore();
								safeZmqSend(backend, nextFrame, more ? ZMQ.SNDMORE : 0);
							}

						}
						else {
							safeZmqSend(backend, firstDataFrame, 0);
							// logger.info(" Client msg recvd and sent to " +
							// workAddr);
						}
					}
					catch (Exception e) {
						logger.error("Error whilst handling external,inbound orignated message", e);
					}

				}
				// Handle inbound dealer/worker activity...forward on to waiting
				// clients if originated from a client
				if (items.pollin(1)) {
					//logger.info("items.pollin(1)");
					try {
						String internalAddressPart1;
						byte[] empty;
						byte[] dataFrame1;
						byte[] returnAddressPart2;
						// receive message
						internalAddressPart1 = new String(backend.recv(0));
						empty = backend.recv(0);
						dataFrame1 = backend.recv(0);
						// If from a worker thread telling us they are ready then mark their status and continue
						if (internalAddressPart1.startsWith(workerPrefix)) {
							if (testHighOrderBytesAreEqual(dataFrame1, READY_OPERATION)) {
								int id = Integer.parseInt(internalAddressPart1.replace(workerPrefix, ""));
								updateWorkerStatus(id, 1);
								logger.info(internalAddressPart1 + " ready");
								continue;
							}
							else if (testHighOrderBytesAreEqual(dataFrame1, SHUTDOWN_OPERATION)) {
								// Got a message from a worker thread indicating they
								// have shutdown..Check the status of all the worker threads...if they are all shutdown then we
								// can break and end this main pump thread.
								if (allWorkersShutdown())
									break;
								continue;
							}
						}
						else if (internalAddressPart1.startsWith(SHUTDOWN_TASKNAME) && testHighOrderBytesAreEqual(dataFrame1, SHUTDOWN_OPERATION)) {
							// Check if its a SHUTDOWN message
							logger.info(" shutting down");
							// Send a SHUTDOWN to all worker threads...this
							// gives
							// them a chance to finish any inflight work they
							// might
							// be doing for an external client
							for (int workerId = 0; workerId < workerStatus.length; workerId++) {
								String workAddr = workerPrefix + (workerId);
								backend.send(workAddr.getBytes(), ZMQ.SNDMORE);
								backend.send(EMPTY_FRAME, ZMQ.SNDMORE);
								backend.send(SHUTDOWN_OPERATION, 0);
							}
							inShutDownMode = true;
							continue;
						}
						// If we are here then we are on an actual outward bound
						// response ...so method response data is available to forward back to external waiting callers.
						// DataFrame1 actually contains first part of return address and needs to be
						// remembered.
						byte[] returnAddressPart1 = dataFrame1;
						returnAddressPart2 = backend.recv(0);
						empty = backend.recv(0);
						safeZmqSend(frontend, returnAddressPart1, ZMQ.SNDMORE);
						safeZmqSend(frontend, returnAddressPart2, ZMQ.SNDMORE);
						frontend.send(EMPTY_FRAME, ZMQ.SNDMORE);
						// The rest is are the actual data/parameters. There must be
						// at least 1 more frame so loop forwarding on all subsequent
						// frames to wa
						boolean more = true;
						while (more) {
							byte[] nextFrame = backend.recv(0);
							more = backend.hasReceiveMore();
							safeZmqSend(frontend, nextFrame, more ? ZMQ.SNDMORE : 0);
						}
					}
					catch (Exception e) {
						logger.error("Error whilst handling internal orignated message", e);
					}
				}
				if (inShutDownMode == true) {
					// Check the status of all the worker threads...if they are
					// all complete then then we can break and end this thread
					if (allWorkersShutdown())
						break;
				}

			}

			// Clean up
			frontend.setLinger(0);
			frontend.close();
			backend.setLinger(0);
			backend.close();
			// context.term();

			logger.info("serverside" + thisInstanceId + " rmi shutdown complete");
		}
	}

	/**
	 * @author andy
	 */
	class WorkerThread extends Thread {
		String name;
		Context context;
		String prefix;
		int id;
		// Used for reflection lookup..
		Class parameterTypes[] = new Class[1];

		WorkerThread(Context context, String prefix, int id) {
			this.context = context;
			this.prefix = prefix;
			this.id = id;
			name = prefix + id;
			// Used for reflection lookup..all remote methods take one argument
			// List<byte[]>
			parameterTypes[0] = List.class;
		}

		public void run() {
			queueHandler.lock.lock();
			queueHandler.lock.unlock();

			Thread.currentThread().setName(name);
			// Prepare our context and sockets
			Socket worker = context.socket(ZMQ.REQ);

			worker.setIdentity(name.getBytes()); // Makes tracing easier

			worker.connect(internalSocketName);
			logger.info(name + " connected");
			// Tell backend we're ready for work
			worker.send(READY_OPERATION, 0);

			while (true) {
				byte[] returnAddressPart1;
				byte[] returnAddressPart2 = null;
				byte[] empty = null;
				byte[] threadRequestIdBytes = null;
				List<byte[]> paramsAsBytes = new ArrayList<>();
				returnAddressPart1 = worker.recv(0);
				long timeReceived = System.nanoTime();
				// Check if time for this thread to shutdown
				if (testHighOrderBytesAreEqual(returnAddressPart1, SHUTDOWN_OPERATION)) {
					// Reply back and exit loop
					updateWorkerStatus(id, 3);
					worker.send(SHUTDOWN_OPERATION, 0);
					break;
				}
				// Indicate this worker is busy
				updateWorkerStatus(id, 2);
				try {
					// logger.info(" " + name + " receiving");
					if (worker.hasReceiveMore())
						returnAddressPart2 = worker.recv(0);
					// frame3 should be empty
					if (worker.hasReceiveMore())
						empty = worker.recv(0);
					if (worker.hasReceiveMore())
						threadRequestIdBytes = worker.recv(0);
					// Starting at next frame is the wrapper object containing
					// everything
					while (worker.hasReceiveMore()) {
						paramsAsBytes.add(worker.recv(0));
					}
					List<byte[]> responseList = new ArrayList<>();
					// 3rd param is serialization id
					String serializationId = new String(paramsAsBytes.get(2));
					try {
						// Look up service/method
						String serviceName = new String(paramsAsBytes.get(0));
						String methodName = new String(paramsAsBytes.get(1));
						ServiceMethods sm = exposedServices.get(serviceName);
						if (sm == null) {
							// Not such service has been registered..
							responseList.add(ZERO_AS_BYTES);
							// TODO add serialized exception
							responseList.add("ServiceNotRegisteredException".getBytes());
						}
						else {
							MethodWrapper mw;
							if ((mw = sm.methods.get(methodName)) == null) {
								responseList.add(ZERO_AS_BYTES);
								// TODO add serialized exception
								responseList.add("MethodNotRegisteredException".getBytes());
							}
							else {
								// We have a valid service and method..so remove
								// the redundant entries from params and pass to the ACTUAL called method
								paramsAsBytes.remove(0);
								paramsAsBytes.remove(0);
								mw.callCount.incrementAndGet();
								long t1 = System.nanoTime();
								List<byte[]> fullResponse = (List<byte[]>) mw.method.invoke(sm.service, paramsAsBytes);
								long t2 = System.nanoTime() - t1;
								mw.cummulativeTime.addAndGet(t2);
								responseList.add(0, ONE_AS_BYTES);
								//Add time taken as next 8 bytes
								responseList.add(convertLongToBytes(t2));
								for (byte[] entry : fullResponse) {
									responseList.add(entry);
								}
							}
						}
					}
					catch (Throwable e) {
						// Any exception is caught and handled and info
						// passed back to caller, 0 means exception...do not
						// want this thread to terminate...always want it to
						// respond
						Throwable ee;
						if (e instanceof InvocationTargetException) {
							ee = ((InvocationTargetException) e).getCause();
						}
						else {
							ee = e;
						}
						logger.error("Error invoking service method - return exception to caller", ee);
						responseList.add(ZERO_AS_BYTES);
						// Add the class name of exception
						responseList.add(ee.getClass().getCanonicalName().getBytes());
						// Try and serialize the exception..if that fails then
						// add the message
						// TODO need a better way of getting to serialization id
						try {
							responseList.add(NimrodObjectSerializer.serialize(serializationId, ee));
						}
						catch (Throwable t) {
							if (ee.getMessage() != null)
								responseList.add(ee.getMessage().getBytes());
							else
								responseList.add(ee.getClass().getName().getBytes());
						}
					}

					// At this point responseList MUST have some content..so
					// construct and send response ... stop if any of the
					// responses are in fact null
					try {
						safeZmqSend(worker, returnAddressPart1, ZMQ.SNDMORE);
						safeZmqSend(worker, returnAddressPart2, ZMQ.SNDMORE);
						worker.send(empty, ZMQ.SNDMORE);
						safeZmqSend(worker, threadRequestIdBytes, ZMQ.SNDMORE);
						// logger.info(Thread.currentThread().getName()+
						// " response ="+new String(serviceNameBytes ));
						for (int j = 0; j < responseList.size(); j++) {
							safeZmqSend(worker, responseList.get(j), j < responseList.size() - 1 ? ZMQ.SNDMORE : 0);

						}
					}
					catch (Exception e) {
						logger.error("Error whilst returning response to main pump", e);
					}

				}
				finally {
					// Indicate this worker is finished
					updateWorkerStatus(id, 1);
				}
			}

			logger.info(name + " exiting...");
		}
	}

	/**
	 * Utilty to check bytes validity passed in by ZMQ from one socket before
	 * being passed on to next socket. Null is possible and causes send a big
	 * problem.
	 *
	 * @param socket
	 * @param msg
	 * @param sendMore
	 */
	static void safeZmqSend(Socket socket, byte[] msg, int sendMore) throws InvalidParameterException {
		// byte[] copyBytes = new byte[msg.length];
		// System.arraycopy(msg, 0, copyBytes, 0, msg.length);
		if (msg == null)
			throw new InvalidParameterException();
		socket.send(msg, sendMore);
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	protected String getDefaultInstanceName() {
		return "zmqServer";
	}

	public void setWorkerThreadPoolSize(int workerThreadPoolSize) {
		this.workerThreadPoolInitialSize = workerThreadPoolSize;
	}

	public void addExposedService(Object service, String[] methods) throws SecurityException, NoSuchMethodException {
		ServiceMethods sm = new ServiceMethods();
		exposedServices.put(service.getClass().getSimpleName(), sm);
		sm.service = service;
		sm.methods = new HashMap<>();
		for (String method : methods) {
			MethodWrapper mw = new MethodWrapper();
			mw.method = sm.service.getClass().getMethod(method, parameterTypes);
			sm.methods.put(method, mw);
		}
	}

	public void addExposedService(final String name, final Object service) {
		ServiceMethods sm = new ServiceMethods();
		// See if there is an explicit serviceName supplied..if not default to
		// the class name
		if (name != null) {
			sm.serviceName = name;
		} else if (service.getClass().isAnnotationPresent(ExposedServiceName.class)) {
			sm.serviceName = service.getClass().getAnnotation(ExposedServiceName.class).serviceName();
		}
		else {
			sm.serviceName = service.getClass().getSimpleName();
		}
		sm.service = service;
		sm.methods = new HashMap<>();
		exposedServices.put(sm.serviceName, sm);
		// Loop thru the methods adding any that are exposed
		Method[] methods = service.getClass().getMethods();
		for (Method method : methods) {
			// System.out.println(method.getName());
			if (method.isAnnotationPresent(ExposedMethod.class)) {
				String methodName = method.getName();
				MethodWrapper mw = new MethodWrapper();
				mw.method = method;
				sm.methods.put(methodName, mw);
				logger.info("Service : " + sm.serviceName + " hosted by class : " + sm.service.getClass().getSimpleName() + " exposes method : " + methodName);
			}
		}
		if (sm.methods.size() == 0)
			exposedServices.remove(service.getClass().getSimpleName());
	}

	public void addExposedService(Object service) {
		addExposedService(null, service);
	}

	public boolean initialize() throws Exception {
		if (super.initialize() == false)
			return false;

		if (workerThreadPoolInitialSize == -1) {
			workerThreadPoolInitialSize = Integer.parseInt(System.getProperty("threadPoolSize", Integer.toString(ZeroMQCommon.GetDefaultThreadPoolSize())));
		}
		if (workerThreadPoolInitialSize == 1) {
			workerThreadPoolInitialSize = 2;
		}
		workerStatus = new int[workerThreadPoolInitialSize];
		workerSafetyMargin = (int) (0.25 * workerThreadPoolInitialSize);
		if (workerSafetyMargin == 0) {
			workerSafetyMargin = 1;
		}

		internalSocketName = INTERNAL_SOCKET_NAME_PREFIX + "-" + getInstanceName() + "-" + thisInstanceId;
		workerPrefix = WORKER_PREFIX + getInstanceName() + "-" + thisInstanceId + "-";
		// Start a dedicated thread to manage the inbound and outbound queues
		queueHandler = new QueueHandlerThread();
		queueHandlerThread = new Thread(queueHandler, PUMP_PREFIX + getInstanceName() + "-" + thisInstanceId);
		queueHandlerThread.start();
		// Delay for a short time to allow queueHandler to acquire lock
		try {
			Thread.sleep(1000);
		}
		catch (InterruptedException e) {
		}

		createWorkerThreads();
		if (useAgent) {
			initializeAgent();
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
		queueHandler.stop();

	}

	/**
	 * There is only ever one thread calling this so done need to worry about
	 * thread safe etc
	 *
	 * @return
	 */
	int getFreeWorker() {
		int entry = 0;
		for (; entry < workerStatus.length; entry++) {
			if (workerStatus[entry] == 1)
				break;
		}
		if (entry >= (workerStatus.length - workerSafetyMargin)) {
			// Time to increase workThread pool..
			try {
				increaseWorkerThreads();
			}
			catch (Throwable t) {
				return -1;
			}
		}
		return entry;
	}

	void createWorkerThreads() {
		int workerId;
		for (workerId = 0; workerId < workerStatus.length; workerId++) {
			new WorkerThread(context, workerPrefix, workerId).start();
		}
	}

	void increaseWorkerThreads() {
		try {
			workerThreadLock.lock();
			logger.info("Increasing workerThreads size by " + workerThreadPoolInitialSize);
			// Quickly extend array and transfer current values..increase in
			// increments of workerThreadPoolInitialSize
			int[] workerStatusNew = new int[workerStatus.length + workerThreadPoolInitialSize];
			int startWorkerId = workerStatus.length;
			for (int i = 0; i < workerStatus.length; i++) {
				workerStatusNew[i] = workerStatus[i];
			}
			workerStatus = workerStatusNew;
			workerThreadLock.unlock();
			// Kick of the new threads for the new workers..
			for (int workerId = startWorkerId; workerId < workerStatusNew.length; workerId++) {
				new WorkerThread(context, workerPrefix, workerId).start();
			}
		}
		finally {
			if (workerThreadLock.isHeldByCurrentThread())
				workerThreadLock.unlock();
		}
	}

	void updateWorkerStatus(int id, int value) {
		try {
			if (workerThreadLock.isLocked()) {
				// This means the the workerThread pool is in process of being
				// increased so this lock forces a wait until the increase is
				// finished.
				workerThreadLock.lock();
			}

			workerStatus[id] = value;
		}
		finally {
			if (workerThreadLock.isHeldByCurrentThread())
				workerThreadLock.unlock();
		}

	}

	/**
	 * When all the worker threads' status is 3 then they are all shutdown
	 *
	 * @return
	 */
	boolean allWorkersShutdown() {
		for (int workerStatu : workerStatus) {
			if (workerStatu != 3)
				return false;
		}
		return true;
	}
}
