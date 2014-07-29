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

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

import com.nimrodtechs.ipc.queue.ConflatingExecutor;
import com.nimrodtechs.ipc.queue.MessageProcessorEntry;
import com.nimrodtechs.ipc.queue.QueueExecutor;
import com.nimrodtechs.ipc.queue.SequentialExecutor;

public class ZeroMQPubSubSubscriber extends ZeroMQCommon {

    final static Logger logger = LoggerFactory.getLogger(ZeroMQPubSubSubscriber.class);

    private static AtomicInteger instanceId = new AtomicInteger(0);
    private int thisInstanceId = instanceId.getAndIncrement();
    private ReentrantLock setupLock = new ReentrantLock();
    private Condition setupCondition = setupLock.newCondition();
    private String internalSocketName;
    private boolean setupCompleted = false;
    private Thread subscriberThread;
    private boolean keepRunning = true;
    protected ConcurrentMap<String, List<? extends MessageReceiverInterface>> listenersBySubjectMap = new ConcurrentHashMap<String, List<? extends MessageReceiverInterface>>();
    protected ConcurrentMap<String, MessageProcessorEntry> messageProcessorEntries = new ConcurrentHashMap<String, MessageProcessorEntry>();
    protected List<String> wildcardSubjects = new ArrayList<String>();
    protected QueueExecutor sequentialExecutor = null;
    public QueueExecutor getSequentialExecutor() {
        if(sequentialExecutor == null) {
            sequentialExecutor = new SequentialExecutor();
            sequentialExecutor.setThreadNamePrefix(instanceName);
            sequentialExecutor.initialize();
        }
        return sequentialExecutor;
    }
    protected QueueExecutor conflatingExecutor = null;
    public QueueExecutor getConflatingExecutor() {
        if(conflatingExecutor == null) {
            conflatingExecutor = new ConflatingExecutor();
            conflatingExecutor.setThreadNamePrefix(instanceName);
            conflatingExecutor.initialize();
        }
        return conflatingExecutor;
    }
    
    @Override
    protected String getDefaultInstanceName() {
        return "zmqSubscriber";
    }

    public boolean initialize() throws Exception {
        if(super.initialize() == false)
            return false;

        internalSocketName = INTERNAL_SOCKET_NAME_PREFIX + "-" + getInstanceName() + "-" + thisInstanceId;

        SubscriberThreadHandler subscriberHandler = new SubscriberThreadHandler();

        subscriberThread = new Thread(subscriberHandler, SUBSCRIBER_PREFIX + getInstanceName() + "-" + thisInstanceId);
        try {
            setupLock.lock();
            subscriberThread.start();
            setupCondition.await(5000, TimeUnit.MILLISECONDS);
            if (setupCompleted == false)
                logger.error("Problem Starting SubscriberThreadHandler");

        } catch (InterruptedException e) {
            logger.error("Starting SubscriberThreadHandler", e);
        } finally {
            if (setupLock.isHeldByCurrentThread())
                setupLock.unlock();
        }
        //Notify the outside world that this instance is running
        initializeAgent();
        
        logger.info("Initialized connection on " + clientSocket);
        return true;
    }

    public void dispose() {
        logger.warn(getInstanceName() + " dispose called");
        super.dispose();
        keepRunning = false;
        // subscriberThread.interrupt();
        Socket client;
        synchronized (context) {
            client = context.socket(ZMQ.REQ);
            // client = context.socket(ZMQ.PUB);
        }
        client.connect(internalSocketName);
        client.send("STOP".getBytes(), 0);
        logger.warn("ZmqMessageSubscriber sent STOP message");
        client.recv(0);
        logger.warn("ZmqMessageSubscriber recvd STOP ack");
    }
    
    public void subscribe(String aSubject, MessageReceiverInterface listener, Class payloadClass, int executorType) {
        //Pass on with the default serializationFormatId
        subscribe(aSubject, listener, payloadClass, defaultSerializerId,executorType);
    }
   
    public void subscribe(String aSubject, MessageReceiverInterface listener, Class payloadClass) {
        //Pass on with the default serializationFormatId
        subscribe(aSubject, listener, payloadClass, defaultSerializerId,QueueExecutor.SEQUENTIAL_QUEUE);
    }
    
    public void subscribe(String aSubject, MessageReceiverInterface listener, Class payloadClass, String serializationFormatId, int executorType) {
        String subject;
        boolean wildcard = false;
        if (aSubject.endsWith("*")) {
            // This is a wildcard matching subject .. so some special treatment needed..
            subject = aSubject.replace("*", "");
            wildcard = true;
        } else {
            subject = aSubject;
        }
        synchronized (listenersBySubjectMap) {
            List<MessageReceiverInterface> list = (List<MessageReceiverInterface>) listenersBySubjectMap.get(subject);
            if (list == null) {
                list = new ArrayList<MessageReceiverInterface>();
                listenersBySubjectMap.put(subject, list);
            }
            list.add(listener);
            if (list.size() == 1) {
                // Setup a queue for messages for this subject
                MessageProcessorEntry mpe = new MessageProcessorEntry();
                // mpe.setPayloadClass(payloadClass);
                mpe.setPayloadClass(payloadClass);
                mpe.setSerializationFormatId(serializationFormatId);
                if (executorType == QueueExecutor.CONFLATING_QUEUE)
                    mpe.setQueueExecutor(getConflatingExecutor());
                else
                    mpe.setQueueExecutor(getSequentialExecutor());
                messageProcessorEntries.put(subject, mpe);
                if (wildcard) {
                    mpe.setWildcardSubscription(true);
                    wildcardSubjects.add(subject);
                }
                // From the subject work out which transport to use...match on
                // prefix
                subscribe(subject);
            } else {
                //TODO Check the parameters passed are same as previous..throw exception if not
                MessageProcessorEntry mpe = messageProcessorEntries.get(subject);
                if(mpe.getSerializationFormatId().equals(serializationFormatId) == false) {
                    
                }
                if(mpe.getPayloadClass().equals(payloadClass) == false) {
                    
                }
            }
        }
        // Publish message to trigger lastValueCache publish for subject
        if (wildcard == false && subject.startsWith(AGENT_SUBJECT_PREFIX) == false && agentPublisher != null) {
            agentPublisher.publish(AGENT_SUBJECT_PREFIX+INITIAL_VALUES_SUFFIX, subject);
            logger.info("requested initial values for " + subject);
        }

    }

    /**
     * Unsubscribes listener from topic
     * 
     * @param subject
     * @param listener
     */
    public void unsubscribe(String aSubject, MessageReceiverInterface listener) {
        String subject;
        if (aSubject.endsWith("*")) {
            // This is a wildcard matching subject .. so some special treatment
            // needed..
            subject = aSubject.replace("*", "");
        } else {
            subject = aSubject;
        }
        synchronized (listenersBySubjectMap) {
            List<MessageReceiverInterface> list = (List<MessageReceiverInterface>) listenersBySubjectMap.get(subject);
            if (list == null) {
                logger.warn("unsubscribe : did not find subject [" + subject + "]");
                return;
            }
            // find the listener in the list thru iterator and remove...
            for (ListIterator<MessageReceiverInterface> itr = list.listIterator(); itr.hasNext();) {
                if (itr.next() == listener) {
                    logger.warn("unsubscribe : subject [" + subject + "] removing listener " + listener);
                    itr.remove();
                    break;
                }
            }
            // list.remove(listener);
            if (list.size() == 0) {
                logger.info("unsubscribe : subject [" + subject + "] count is now 0 so unsubscribe");
                // Do the actual unsubscribe from transport
                if (listenersBySubjectMap.remove(subject) == null)
                    logger.warn("unsubscribe : subject [" + subject + "] count was 0 but entry missing");
                MessageProcessorEntry mpe = messageProcessorEntries.remove(subject);
                if (mpe != null) {
                    unsubscribe(subject);
                    if (mpe.isWildcardSubscription()) {
                        wildcardSubjects.remove(subject);
                    }
                }
            }
        }
    }

    /**
     * Check to see if subject already subscribed to..if not then actually
     * subscribe and add listener Otherwise just add another listener..
     * 
     * @param subject
     * @param listener
     */
    private void subscribe(String subject) {
        if (setupCompleted == false) {
            // Not quite ready so pause via lock object..when setup is completed
            // the lock will be release and so will this..
            setupLock.lock();
            // Immediately unlock as its done its job of pausing our first
            // subscription
            setupLock.unlock();
        }
        logger.info("transport subscribe : first subscription for subject " + subject);
        // Its the first occurance of this subject so tell
        // SubscriberThreadHandler to add the subscription..
        Socket client;
        synchronized (context) {
            client = context.socket(ZMQ.REQ);
            // client = context.socket(ZMQ.PUB);
        }
        client.connect(internalSocketName);

        // Communicate to the main subscriber thread to add this
        // subscription..
        client.send("ADD".getBytes(), ZMQ.SNDMORE);
        client.send(subject.getBytes(), 0);
        logger.info("subscribe : sent ADD message for subject " + subject);
        // Receive the acknowledgement
        client.recv(0);
        logger.info("subscribe : recvd ack for subject " + subject);
        client.close();
    }

    /**
     * Unsubscribe
     * 
     * @author andy
     * 
     */
    private void unsubscribe(String subject) {
        logger.info("unsubscribe : subject [" + subject + "] count is now 0 so unsubscribe");
        // If there are now more listeners then remove all aspects of
        // the subscription
        Socket client;
        synchronized (context) {
            // client = context.socket(ZMQ.PUB);
            client = context.socket(ZMQ.REQ);
        }
        client.connect(internalSocketName + thisInstanceId);

        // Communicate to the main subscriber thread to remove this
        // subscription..
        client.send("REMOVE".getBytes(), ZMQ.SNDMORE);
        client.send(subject.getBytes(), 0);
        // Receive the ack
        client.recv(0);
        logger.info("unsubscribe : recvd ack for REMOVE subject [" + subject + "]");
        client.close();
    }

    /**
     * Thread dedicated to reading external messages..subscribe and unsubscribe
     * has to be handled by this thread hence the usage of the internalSocket so
     * that other threads can communicate their desire to subscribe or
     * unsubscribe.
     * 
     * @author andy
     * 
     */
    class SubscriberThreadHandler implements Runnable {
        int totalMsgs;

        @Override
        public void run() {
            setupLock.lock();
            logger.info("SubscriberThreadHandler starting2...");
            // Aquire a lock to block any subscriptions until setup is
            // completed...
            ZMQ.Socket externalSocket = context.socket(ZMQ.SUB);
            try {
                if (manyToOne) {
                    externalSocket.bind(clientSocket);
                    logger.info("manyToOne so bind to " + clientSocket);
                } else {
                    externalSocket.connect(clientSocket);
                    logger.info("oneToMany so connect to " + clientSocket);
                }
            } catch (Throwable t) {
                logger.error("Error starting SubscriberThreadHandler", t);
                return;
            }
            ZMQ.Socket internalSocket = context.socket(ZMQ.ROUTER);

            internalSocket.bind(internalSocketName);
            // Ensure we pickup any and all messages sent internally
            // internalSocket.subscribe(new byte[0]);
            // Initialize poll set
            Poller items = new ZMQ.Poller(2);

            // Always poll internal subscription requests
            items.register(internalSocket, Poller.POLLIN);
            // Always poll for external message delivery
            items.register(externalSocket, Poller.POLLIN);
            // We are ready for business
            setupCompleted = true;
            setupCondition.signal();
            setupLock.unlock();
            // Main loop
            while (!Thread.currentThread().isInterrupted() && keepRunning) {
                items.poll();
                // logger.info("SubscriberThreadHandler : recvd something..");
                if (items.pollin(0)) {
                    // logger.info("SubscriberThreadHandler : recvd something..on items.pollin(0) ");
                    // A new subscription to be added or existing one to be
                    // removed
                    // Store the first 2 frames
                    byte[] firstFrame = internalSocket.recv(0);
                    byte[] secondFrameEmpty = internalSocket.recv(0);
                    String addOrRemove = new String(internalSocket.recv(0));
                    if (addOrRemove.equals("STOP")) {
                        // SHUTDOWN
                        logger.info("SubscriberThreadHandler stopping..");
                        // break;
                    } else {
                        byte[] subject = internalSocket.recv(0);
                        if (addOrRemove.equals("ADD")) {
                            externalSocket.subscribe(subject);
                            logger.info("ADDED subscription [" + new String(subject) + "]");
                        } else if (addOrRemove.equals("REMOVE")) {
                            externalSocket.unsubscribe(subject);
                            logger.info("REMOVED subscription [" + new String(subject) + "]");
                        } else {
                            logger.info("UNKNOWN command [" + addOrRemove + "]");
                        }
                    }
                    // Reply back
                    internalSocket.send(firstFrame, ZMQ.SNDMORE);
                    internalSocket.send(secondFrameEmpty, ZMQ.SNDMORE);
                    internalSocket.send("ACK".getBytes(), 0);
                    logger.info("SubscriberThreadHandler send ack for " + addOrRemove);
                    if (addOrRemove.equals("STOP"))
                        break;
                }
                if (items.pollin(1)) {
                    boolean hasMore = true;
                    while (hasMore) {
                        // logger.info("SubscriberThreadHandler : recvd something..on items.pollin(1) ");
                        // A new message has arrived..delegate to a thread and
                        // which
                        // in turn will call listeners
                        String subject = new String(externalSocket.recv(0));
                        byte[] message = externalSocket.recv(0);
                        totalMsgs++;
                        // Dispatch the new message to the registered
                        // subscribers
                        dispatch(subject, message);
                        hasMore = externalSocket.hasReceiveMore();
                    }
                }
            }
            items.unregister(internalSocket);
            items.unregister(externalSocket);
            // Set linger to 0 is very important otherwise sockets hang around
            // using up OS file descriptors
            internalSocket.setLinger(0);
            internalSocket.close();
            internalSocket = null;
            externalSocket.setLinger(0);
            externalSocket.close();
            externalSocket = null;
            logger.info("SubscriberThreadHandler stopped. Total Msgs=" + totalMsgs);
        }
    }

    void dispatch(String subject, byte[] message) {
        MessageProcessorEntry mpe = messageProcessorEntries.get(subject);
        if (mpe != null) {
            // Pass on to the appropriate executor to process the message
            mpe.getQueueExecutor().process(subject, subject, message, mpe, listenersBySubjectMap);
        }
        if (wildcardSubjects.size() > 0) {
            for (String wildcardSubject : wildcardSubjects) {
                if (subject.length() > wildcardSubject.length()) {
                    if (subject.substring(0, wildcardSubject.length()).equals(wildcardSubject)) {
                        mpe = messageProcessorEntries.get(wildcardSubject);
                        if (mpe != null) {
                            // Pass on to the appropriate executor to process
                            // the message
                            mpe.getQueueExecutor().process(wildcardSubject, subject, message, mpe, listenersBySubjectMap);
                        }
                    }
                }
            }
        }
    }
    class MessageWrapper {
        public MessageWrapper(String actualSubject, byte[] rawMessage) {
            super();
            this.actualSubject = actualSubject;
            this.rawMessage = rawMessage;
        }
        String actualSubject;
        byte[] rawMessage;
    }

}
