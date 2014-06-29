package com.nimrodtechs.ipc.queue;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.nimrodtechs.ipc.MessageReceiverInterface;

/**
 * Simple FIFO queue handling...new messages get added to end of queue..every
 * message for same subject gets process sequentially by same thread in the
 * event there is a backlog... or delegate to a new thread if there is no
 * current thread for the subject. Note : subject is the unit of workflow
 * control. Actual Subject is the actual subject of message. E.g. subject might
 * be xs.rate* i.e. a wildcard, actual subject might be xs.rate.EUR.USD.SPOT or
 * 
 * @author andy
 *
 */
public class SequentialExecutor extends QueueExecutor {
    @Override
    public void process(String subject, String actualSubject, byte[] message, MessageProcessorEntry mpe, ConcurrentMap<String, List<? extends MessageReceiverInterface>> listeners) {
        // This is the sequential flavor so just add to end of current list
        mpe.messages.offer(new MessageWrapper(actualSubject, message));
        if (mpe.getInprogressIndicator().compareAndSet(false, true)) {
            // A current thread is not inprogress so start one
            serviceThreads.execute(new ServiceMessageTask(subject, mpe, listeners));
        }
    }
}
