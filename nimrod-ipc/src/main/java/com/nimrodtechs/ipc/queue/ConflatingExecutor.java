package com.nimrodtechs.ipc.queue;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.nimrodtechs.ipc.MessageReceiverInterface;

/**
 * There should never be more than 2 entries in a queue...the one being worked
 * on and the latest arrived. Every message for same subject gets process
 * sequentially by same thread in the event there is a backlog... or delegate to
 * a new thread if there is no current thread for the subject.
 * Note : a wildcard subscription will be processed in one queue therefore will be subject to conflation, which might not be what
 * you actually want.
 * 
 * @author andy
 *
 */
public class ConflatingExecutor extends QueueExecutor {
    @Override
    public void process(String subject, String actualSubject, byte[] message, MessageProcessorEntry mpe, ConcurrentMap<String, List<? extends MessageReceiverInterface>> listeners) {
        // This is the conflating flavor so as a result of this there can only
        // be 1 or 2 entries i.e. if there
        // is one already the
        if (mpe.getInprogressIndicator().compareAndSet(false, true)) {
            mpe.conflatedMessages[0] = new MessageWrapper(actualSubject, message);
            mpe.conflatedMessages[1] = null;
            // A current thread is not inprogress so start one
            serviceThreads.execute(new ServiceMessageTask(subject, mpe, listeners));
        } else {
            // Either slot 0 or 1 is free...
            if (mpe.conflatedMessages[0] == null)
                mpe.conflatedMessages[0] = new MessageWrapper(actualSubject, message);
            else if (mpe.conflatedMessages[1] == null)
                mpe.conflatedMessages[1] = new MessageWrapper(actualSubject, message);
        }
    }
}