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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageProcessorEntry {
    protected AtomicBoolean inProgressIndicator = new AtomicBoolean(false);
    //protected Queue<MessageWrapper> messages = new ConcurrentLinkedQueue<MessageWrapper>();
    protected Queue<MessageWrapper> messages = new ArrayBlockingQueue<>(QueueExecutor.MAX_QUEUE);
    protected MessageWrapper[] conflatedMessages = new MessageWrapper[2];
    protected QueueExecutor queueExecutor;
    protected String serviceName;
    protected Class<?> payloadClass;
    protected String serializationFormatId;
    protected boolean wildcardSubscription = false;

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public Class<?> getPayloadClass() {
        return payloadClass;
    }

    public void setPayloadClass(Class<?> payloadClass) {
        this.payloadClass = payloadClass;
    }

    public String getSerializationFormatId() {
        return serializationFormatId;
    }

    public void setSerializationFormatId(String serializationFormatId) {
        this.serializationFormatId = serializationFormatId;
    }

    public boolean isWildcardSubscription() {
        return wildcardSubscription;
    }

    public void setWildcardSubscription(boolean wildcardSubscription) {
        this.wildcardSubscription = wildcardSubscription;
    }

    public AtomicBoolean getInprogressIndicator() {
        return inProgressIndicator;
    }

    public Queue<MessageWrapper> getMessages() {
        return messages;
    }

    public QueueExecutor getQueueExecutor() {
        return queueExecutor;
    }

    public void setQueueExecutor(QueueExecutor queueExecutor) {
        this.queueExecutor = queueExecutor;
    }

}
