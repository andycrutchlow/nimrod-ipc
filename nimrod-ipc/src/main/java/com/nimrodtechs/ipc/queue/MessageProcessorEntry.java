package com.nimrodtechs.ipc.queue;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageProcessorEntry {
    protected AtomicBoolean inProgressIndicator = new AtomicBoolean(false);
    protected Queue<MessageWrapper> messages = new ConcurrentLinkedQueue<MessageWrapper>();
    protected MessageWrapper[] conflatedMessages = new MessageWrapper[2];
    protected QueueExecutor queueExecutor;
    protected String serviceName;

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    protected Class<?> payloadClass;

    public Class<?> getPayloadClass() {
        return payloadClass;
    }

    public void setPayloadClass(Class<?> payloadClass) {
        this.payloadClass = payloadClass;
    }

    protected String serializationFormatId;

    public String getSerializationFormatId() {
        return serializationFormatId;
    }

    public void setSerializationFormatId(String serializationFormatId) {
        this.serializationFormatId = serializationFormatId;
    }

    protected boolean wildcardSubscription = false;

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
