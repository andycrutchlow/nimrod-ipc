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

package com.nimrodtechs.ipc.queue;

import com.nimrodtechs.exceptions.NimrodSerializationException;
import com.nimrodtechs.ipc.MessageReceiverInterface;
import com.nimrodtechs.ipc.ZeroMQCommon;
import com.nimrodtechs.serialization.NimrodObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Generalized executor service that manages threading on a per subject basis.
 * Note : this means messages arriving as result of wildcard subscriptions will
 * be handled by same thread which means the listener needs to handle splitting
 * of concurrent work and queue logic.
 *
 * @author andy
 */
public abstract class QueueExecutor implements UncaughtExceptionHandler {
    public final static int SEQUENTIAL_QUEUE = 0;
    public final static int CONFLATING_QUEUE = 1;
    public final static int MAX_QUEUE = 5096;
    final static Logger logger = LoggerFactory.getLogger("QueueExecutor");
    protected int warningThreshold = MAX_QUEUE - 100;
    /**
     * Threads created dynamically as needed. There cannot be more threads than
     * subjects subscribed. There should be a lot less though.
     */
    protected ThreadPoolExecutor serviceThreads;
    int logCount = 0;
    private String threadNamePrefix = "qxf-Dispatcher";
    // Different ways of assigning this value..start with a default which can be
    // override thru commandline vm args
    private int threadPoolSize = System.getProperty("subscriptionThreadPoolSize") != null ? Integer.parseInt(System.getProperty("subscriptionThreadPoolSize")) : (ZeroMQCommon.GetDefaultThreadPoolSize());
    private int threadPoolType = 0;

    class TF implements ThreadFactory {
        UncaughtExceptionHandler handler;
        // Only one thread will ever be invoking the factory so dont need to
        // make count threadsafe...
        // AtomicInteger count = new AtomicInteger(1);
        int count = 1;

        TF(UncaughtExceptionHandler handler) {
            this.handler = handler;
        }

        public synchronized Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(threadNamePrefix + "-" + count++);
            t.setUncaughtExceptionHandler(handler);
            return t;
        }
    }

    /**
     * This ensures messages for the same subject that arrive in short/same
     * period of time get processed in order by the same thread if there is a
     * backlog. A later message cannot over take an earlier one no matter how
     * long a messageReceived method takes.
     *
     * @author andy
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    protected final class ServiceMessageTask implements Runnable {
        MessageProcessorEntry mpe;
        String subject;
        // String actualSubject;
        ConcurrentMap<String, List<? extends MessageReceiverInterface>> listeners;

        ServiceMessageTask(String subject, MessageProcessorEntry mpe, ConcurrentMap<String, List<? extends MessageReceiverInterface>> listeners) {
            this.subject = subject;
            // this.actualSubject = actualSubject;
            this.mpe = mpe;
            this.listeners = listeners;
        }

        /**
         * This thread is dedicated to processing all messages for a particular
         * subject for a period of time whilst there are current messages to be
         * consumed for the subject
         */
        @Override
        public void run() {
            MessageWrapper messageWrapper;
            // When there is no more outstanding work then exit this thread
            while ((messageWrapper = getNextMessage(mpe)) != null) {
                if (messageWrapper.rawMessage == null || messageWrapper.rawMessage.length == 0) {
                    logger.error("subject=[" + subject + "] Payload empty so ignore");
                    continue;
                }
                Object messageAsObject = null;
                Object messageToPassOn = null;
                List<? extends MessageReceiverInterface> list = listeners.get(subject);
                if (list != null) {
                    int size = list.size();

                    for (int i = 0; i < list.size(); i++) {
                        MessageReceiverInterface aList = list.get(i);
                        try {
                            // Get the payloadclass for this subject/listener
                            Class payLoadClass = mpe.payloadClass;
                            // Only deserialize once ...don't deserialize at all
                            // if the payload class is byte[]
                            if (payLoadClass == null || payLoadClass.isInstance(messageWrapper.rawMessage))
                                messageToPassOn = messageWrapper.rawMessage;
                            else {
                                if (messageAsObject == null) {
                                    try {
                                        messageAsObject = deserialize(messageWrapper.rawMessage, payLoadClass, mpe.getSerializationFormatId());
                                    }
                                    catch (Throwable e) {
                                        logger.error("Failed to deserialize payload to class " + payLoadClass.getSimpleName() + " on subject " + subject, e);
                                        continue;
                                    }
                                }
                                messageToPassOn = messageAsObject;
                            }
                            if (messageToPassOn == null)
                                logger.error("data on subject " + subject + " is null - ignore..");
                            else {
                                try {
                                    ((MessageReceiverInterface) aList).messageReceived(messageWrapper.actualSubject, messageToPassOn);
                                }
                                catch (IndexOutOfBoundsException iobe) {
                                    //Do nothing .. means that something else has unsubscribed between getting list above and now
                                }
                            }

                        }
                        catch (Throwable t) {

                            try {
                                logger.error("Exception occured calling eventReceived for subject " + subject + " in listenter " + aList.getClass().getName(), t);
                            }
                            catch (Throwable e) {
                                logger.error("Exception occured calling eventReceived for subject " + subject + " in listenter ", t);
                            }
                        }
                    }
                }
            }
            mpe.getInprogressIndicator().set(false);
        }
    }

    public void setThreadNamePrefix(String threadNamePrefix) {
        if (this instanceof ConflatingExecutor)
            this.threadNamePrefix = "qxfc-" + threadNamePrefix;
        else
            this.threadNamePrefix = "qxfs-" + threadNamePrefix;
    }

    public void setWarningThreshold(int warningThreshold) {
        this.warningThreshold = warningThreshold;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public void setThreadPoolType(int threadPoolType) {
        this.threadPoolType = threadPoolType;
    }

    /**
     * More lightweight/dynamic version that will increase/decrease threadpool
     * based on usage.
     */
    public void initialize() {
        // serviceThreads = (ThreadPoolExecutor)
        // Executors.newCachedThreadPool(new TF(this));
        logger.info("initialize: threadPoolSize=" + threadPoolSize);
        serviceThreads = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize, new TF(this));
    }

    public void dispose() {
        if (serviceThreads != null)
            serviceThreads.shutdownNow();
    }

    private MessageWrapper getNextMessage(MessageProcessorEntry mpe) {
        if (mpe.queueExecutor instanceof SequentialExecutor) {
            // if(warningThreshold > 0)
            // {
            // if(mpe.messages.size() > warningThreshold)
            // {
            // if(logCount++ % 1000 == 0)
            // //Only log every 1000th time this condition met
            // logger.warn("Queue size is "+mpe.messages.size()+" which is greater than warning threashold "+warningThreshold);
            // }
            // }
            return mpe.messages.poll();
        }
        else {
            // Conflating queue - so flip between 2 slots ... when both are
            // empty then we are done
            if (mpe.conflatedMessages[0] != null) {
                MessageWrapper o = mpe.conflatedMessages[0];
                mpe.conflatedMessages[0] = null;
                return o;
            }
            else if (mpe.conflatedMessages[1] != null) {
                MessageWrapper o = mpe.conflatedMessages[1];
                mpe.conflatedMessages[1] = null;
                return o;
            }
            else
                return null;
        }
    }

    private Object deserialize(byte[] aMessage, Class payloadClass, String serializationFormatId) throws NimrodSerializationException {
        // Message just consists of a 8 byte time stamp and a serialized data
        // class
        // TODO Do something useful with the timestamp ... update statistics on
        // this subscriber transport, subject (needs to be passed in), message
        // size etc...
        long timestamp = extractLong(aMessage, 0, 8);
        // The payloadClass indicates the desired class to deserialize and
        // return..if its a byte[] then keep intact
        if (payloadClass == byte[].class)
            return aMessage;
        else {
            byte[] actualMessage = new byte[aMessage.length - 8];
            System.arraycopy(aMessage, 8, actualMessage, 0, aMessage.length - 8);
            return NimrodObjectSerializer.deserialize(serializationFormatId, actualMessage, payloadClass);
        }
    }

    private long extractLong(byte[] bytes, int offset, int length) {
        long l = 0;
        for (int i = offset; i < offset + length; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }

    @Override
    public void uncaughtException(Thread thread, Throwable e) {
        logger.error(" thread=" + thread.getName() + " " + e.getMessage(), e);

    }

    abstract public void process(String subject, String actualSubject, byte[] message, MessageProcessorEntry mpe, ConcurrentMap<String, List<? extends MessageReceiverInterface>> listeners);

}
