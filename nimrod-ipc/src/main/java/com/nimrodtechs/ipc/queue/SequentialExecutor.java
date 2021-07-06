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

import com.nimrodtechs.ipc.MessageReceiverInterface;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple FIFO queue handling...new messages get added to end of queue..every
 * message for same subject gets process sequentially by same thread in the
 * event there is a backlog... or delegate to a new thread if there is no
 * current thread for the subject. Note : subject is the unit of workflow
 * control. Actual Subject is the actual subject of message. E.g. subject might
 * be aa.bb* i.e. a wildcard, actual subject might be aa.bb.dd.ee
 *
 * @author andy
 */
public class SequentialExecutor extends QueueExecutor {
    @Override
    public void process(String subject, String actualSubject, byte[] message, MessageProcessorEntry mpe, ConcurrentMap<String, List<? extends MessageReceiverInterface>> listeners) {
        // This is the sequential flavor so just add to end of current list
        if (mpe.messages.size() > warningThreshold) {
            //Log an error and return for now..
            if (mpe.messages.size() == MAX_QUEUE) {
                logger.error(subject + " Queue size is " + mpe.messages.size() + " which is = max size " + MAX_QUEUE + " so skip ... this is serious!!!!");
                return;
            }
            else {
                //Uncomment this if needed...but act of logging will affect flushing queue
                //logger.warn("Queue size is "+mpe.messages.size()+" which is greater than threshold "+warningThreshold);
            }
        }
        mpe.messages.offer(new MessageWrapper(actualSubject, message));
        if (mpe.getInprogressIndicator().compareAndSet(false, true)) {
            // A current thread is not inprogress so start one
            serviceThreads.execute(new ServiceMessageTask(subject, mpe, listeners));
        }
    }
}
