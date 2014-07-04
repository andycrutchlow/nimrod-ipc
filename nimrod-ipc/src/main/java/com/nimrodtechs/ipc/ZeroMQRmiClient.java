package com.nimrodtechs.ipc;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.nimrodtechs.exceptions.NimrodRmiEventListener;
import com.nimrodtechs.exceptions.NimrodRmiException;
import com.nimrodtechs.exceptions.NimrodRmiNotConnectedException;
import com.nimrodtechs.exceptions.NimrodRmiRemoteException;
import com.nimrodtechs.exceptions.NimrodRmiTimeoutException;
import com.nimrodtechs.exceptions.NimrodRmiTransportException;
import com.nimrodtechs.exceptions.NimrodSerializationException;
import com.nimrodtechs.exceptions.NimrodSerializerNotFoundException;
import com.nimrodtechs.serialization.NimrodObjectSerializationInterface;
import com.nimrodtechs.serialization.NimrodObjectSerializer;

/**
 * Class to handle clientside communication to 'rmi' like services hosted by
 * servers identified by one or more externalSocketURL's.
 * 
 * @author andy
 *
 */
public class ZeroMQRmiClient extends ZeroMQCommon {
    private static Logger logger = LoggerFactory.getLogger(ZeroMQRmiClient.class);
    private ZeroMQRmiClient instance;
    private boolean connected = false;
    private Date connectedMadeAtDateTime = null;
    private boolean isFirstTime = true;
    private boolean stopped = false;
    private int BREAKTOLERANCECOUNT = 1;
    private int breakTolerance = BREAKTOLERANCECOUNT;
    private boolean alreadyNotifiedBreak = false;
    private static AtomicInteger instanceId = new AtomicInteger(0);
    private int thisInstanceId = instanceId.getAndIncrement();
    private AtomicLong threadRequestId = new AtomicLong(1);
    private String internalSocketName;
    private QueueTask queueHandler;
    private Thread queueHandlerThread;
    
    protected String getDefaultInstanceName() {
        return "zmqClient";
    }

    private String zmqClientPumpThreadName;
    private String heartBeatThreadName;
    private Thread heartbeatThread;
    private HeartbeatTask heartbeatTask;
    private ConcurrentHashMap<String, InprocConnection> callsInProgress = new ConcurrentHashMap<String, InprocConnection>();
    private ReentrantLock callsInProgressLock = new ReentrantLock();
    private Condition callsInProgressCondition = callsInProgressLock.newCondition();
    private List<NimrodRmiEventListener> eventListeners = new ArrayList<NimrodRmiEventListener>();

   
    private AtomicLong inprocThreadId = new AtomicLong(1);
    private AtomicLong seqNo = new AtomicLong(0);
    private ObjectPool inprocPool = null;
    private int finalPoolSize;
    private int inprocPoolSize = Runtime.getRuntime().availableProcessors() / 2;
    private List<InprocConnection> currentInprocConnections = new ArrayList<InprocConnection>();

    class InprocConnection {
        Socket socket;
        Thread thread;
        String name;
        SendAndReceiveTask task;
        BlockingQueue<List<byte[]>> queueIn = new ArrayBlockingQueue<List<byte[]>>(2);
        BlockingQueue<List<byte[]>> queueOut = new ArrayBlockingQueue<List<byte[]>>(1);
        long timeout;
        boolean isAlive = true;
        boolean alreadyReturnedToPool = false;
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        String threadRequestIdentifier;
    }

    class PoolConnectionFactory extends BasePoolableObjectFactory {
        Context context;

        public PoolConnectionFactory(Context context) {
            this.context = context;
        }

        // Create InprocConnection with thread waiting for some input
        public InprocConnection makeObject() throws Exception {
            final InprocConnection inprocConnection = new InprocConnection();
            try {
                inprocConnection.task = new SendAndReceiveTask(inprocConnection);
            } catch (Exception e1) {
                throw e1;
            }
            inprocConnection.thread = new Thread(inprocConnection.task);
            inprocConnection.lock.lock();
            inprocConnection.thread.start();
            try {
                inprocConnection.condition.await();
            } catch (InterruptedException e) {
            }
            // At this point we know the newly created resource is ready to use
            // to the pool can provide it back to caller
            // logger.info("Pool NumActive="+inprocPool.getNumActive()+" NumIdle="+inprocPool.getNumIdle());
            currentInprocConnections.add(inprocConnection);
            return inprocConnection;

        }

        // when an object is returned to the pool,
        // we'll clear it out
        public void passivateObject(InprocConnection con) {
            logger.info("in passivateObject");
        }

        public boolean validateObject(InprocConnection con) {
            logger.info("in validateObject");
            if (con.isAlive)
                return false;
            else
                return true;
        }

        public void destroyObject(InprocConnection con) {
            logger.info("in destroyObject");
            // Need to terminate the thread which will handle closing the socket
            try {
                con.queueIn.put(new ArrayList<byte[]>());
            } catch (InterruptedException e) {
            }

        }

        // for all other methods, the no-op
        // implementation in BasePoolableObjectFactory
        // will suffice
    }

    public ZeroMQRmiClient() {
        instance = this;
    }

    public void initialize() throws Exception {

        super.initialize();

        logger.info("ZMQ Version : " + ZMQ.getFullVersion());
        internalSocketName = INTERNAL_SOCKET_NAME_PREFIX + "-"+getInstanceName()+"-" + thisInstanceId;
        zmqClientPumpThreadName = PUMP_PREFIX + getInstanceName() + "-" + thisInstanceId;
        heartBeatThreadName = HEARTBEAT_PREFIX + getInstanceName() + "-" + thisInstanceId;

        externalSocketURL.add(clientSocket);
        initializeQueue();
        initializeHeartbeat();
        //Indicates that subsequent calls to initialize is NOT the first time
        isFirstTime = false;
    }

    public void dispose() {
        stopped = true;
        connected = false;
//        if (inprocPool != null)
//            finalPoolSize = inprocPool.getNumActive() + inprocPool.getNumIdle();
        heartbeatTask.stop();
        queueHandler.stopQueue();

    }

    private void initializeQueue() {
        // Check if previous still running
        if (queueHandler != null) {
            if (queueHandler.running == true) {
                // Wait and try again??

            }
        }
        // Start a dedicated thread to manage the inbound and outbound queues
        queueHandler = new QueueTask();
        queueHandlerThread = new Thread(queueHandler, zmqClientPumpThreadName);
        try {
            // Wait until queue pump running is running
            queueHandler.lock.lock();
            queueHandlerThread.start();
            queueHandler.condition.await();
        } catch (Exception e) {
            logger.error("Exception starting queue");
        } finally {
            queueHandler.lock.unlock();
        }

    }

    class QueueTask implements Runnable {
        private ZMQ.Poller poller;
        private boolean running = false;
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        public void stopQueue() {
            // Send a message into the queue pump to tell it to stop
            lock.lock();
            Socket socket = context.socket(ZMQ.REQ);
            socket.setLinger(0);
            socket.setIdentity(SHUTDOWN_TASKNAME.getBytes());
            socket.connect(internalSocketName);
            socket.send(SHUTDOWN_OPERATION, 0);
            
            try {
                condition.await(1000,TimeUnit.MILLISECONDS);
            } catch (InterruptedException e1) {

            }
            socket.close();
            lock.unlock();
        }

        public void run() {
            try {
                logger.info("QueueHandler aquire lock");
                lock.lock();
                logger.info("QueueHandler starting");

                frontend = context.socket(ZMQ.ROUTER);
                frontend.setLinger(0);
                frontend.bind(internalSocketName);
                backend = context.socket(ZMQ.DEALER);
                // Set linger to 0 is very important otherwise sockets hang
                // around using up OS file descriptors
                backend.setLinger(0);
                backend.connect(clientSocket);
                logger.info("Connecting to " + clientSocket);
                // Set inproc pool and threads starting thread id back to 1
                if (inprocPool == null) {
                    inprocThreadId.set(1);
                    inprocPool = new GenericObjectPool(new PoolConnectionFactory(context), inprocPoolSize, GenericObjectPool.WHEN_EXHAUSTED_BLOCK, -1);
                }
                this.poller = new ZMQ.Poller(2);
                this.poller.register(frontend, ZMQ.Poller.POLLIN);
                this.poller.register(backend, ZMQ.Poller.POLLIN);
                byte[] msg = null;
                boolean more = true;
                condition.signal();
                running = true;
                lock.unlock();
                logger.info("QueueHandler started");
                boolean restartBackEnd = false;
                while (running) {
                    try {
                        // wait until there are either requests or replies to
                        // process
                        poller.poll();
                        // logger.info("QueueHandler after poll");
                        // process a request from front end
                        if (poller.pollin(0)) {
                            more = true;
                            // int frameCount = 0;
                            boolean firstFrame = true;
                            String indentity = "";
                            while (more) {
                                // logger.info("QueueHandler before frontend recv");
                                msg = frontend.recv(0);

                                if (firstFrame) {
                                    indentity = new String(msg);
                                    if (indentity.startsWith(SHUTDOWN_TASKNAME)) {
                                        frontend.recv(0);
                                        running = false;
                                        // Assume control of the lock from the
                                        // shutdown method
                                        lock.lock();
                                        break;
                                    } else if (indentity.startsWith(INTERRUPT_CALLS_IN_PROGRESS_TASKNAME)) {
                                        // logger.info("INTERRUPTCALLSINPROGRESSTASKNAME received : connected = "+connected);
                                        frontend.recv(0);
                                        byte[] operation = frontend.recv(0);
                                        if (testHighOrderBytesAreEqual(INTERRUPT_CALLS_IN_PROGRESS_OPERATION, operation))
                                            interruptCallsInProgress();
                                        msg = null;
                                        restartBackEnd = true;
                                    }
                                    firstFrame = false;
                                }
                                // logger.info("QueueHandler after frontend recv");
                                more = frontend.hasReceiveMore();

                                if (msg != null) {
                                    backend.send(msg, more ? ZMQ.SNDMORE : 0);
                                }
                                if (running == false)
                                    // Time to stop loop and thread
                                    continue;
                            }
                        }

                        // process a reply from backend ..
                        if (poller.pollin(1)) {
                            more = true;
                            while (more) {
                                msg = backend.recv(0);

                                more = backend.hasReceiveMore();

                                if (msg != null) {
                                    frontend.send(msg, more ? ZMQ.SNDMORE : 0);
                                }
                            }
                        }
                        if (restartBackEnd) {
                            this.poller.unregister(backend);
                            backend.close();
                            backend = context.socket(ZMQ.DEALER);
                            // Set linger to 0 is very important otherwise
                            // sockets hang around
                            // using up OS file descriptors
                            backend.setLinger(0);
                            backend.connect(clientSocket);
                            // logger.info("Re-Connecting to " +
                            // externalSocketNames.get(currentExternalSocketEntry));
                            this.poller.register(backend, ZMQ.Poller.POLLIN);
                            restartBackEnd = false;
                        }
                    } catch (ZMQException e) {
                        // context destroyed, exit
                        if (ZMQ.Error.ETERM.getCode() == e.getErrorCode()) {
                            break;
                        }
                        throw e;
                    }
                }
                // Send a response on frontEnd socket for all calls in progress
                // i.e. send an Exception message.
                interruptCallsInProgress();

                // logger.info("QueueHandler after synchronized(callsInProgress)");
                poller.unregister(backend);
                poller.unregister(frontend);
                // logger.info("QueueHandler before close backend");
                backend.close();
                backend = null;
                // logger.info("QueueHandler after close backend");
                // logger.info("QueueHandler before close frontend");

                frontend.close();
                frontend = null;

                // logger.info("QueueHandler after close frontend");
                currentExternalSocketEntry++;
                if (currentExternalSocketEntry == externalSocketURL.size())
                    currentExternalSocketEntry = 0;
                logger.info("QueueHandler terminating");
                running = false;
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    // Wake up the waiters e.g. the shutdown task
                    condition.signalAll();
                    lock.unlock();
                }
            }
            logger.info("QueueHandler terminated");
        }
    }
    
    /**
     * Convenience wrapper to assume the value of serializationId
     * @param responseClass
     * @param serviceName
     * @param methodName
     * @param timeout
     * @param parameters
     * @return
     * @throws Exception
     */
    public Object executeRmiMethod(Class<?> responseClass, String serviceName, String methodName, Object... parameters) throws Exception {
        //Assume this first entry in serializers is the default serializer
        return executeRmiMethod(defaultSerializerId,responseClass,serviceName, methodName, -1, parameters);
    }
    
    /**
     * Convenience wrapper to assume the value of serializationId
     * @param responseClass
     * @param serviceName
     * @param methodName
     * @param timeout
     * @param parameters
     * @return
     * @throws Exception
     */
    public Object executeRmiMethod(Class<?> responseClass, String serviceName, String methodName, long timeout, Object... parameters) throws Exception {
        //Assume this first entry in serializers is the default serializer
        return executeRmiMethod(defaultSerializerId,responseClass,serviceName, methodName, timeout, parameters);
    }

    
    /**
     * Common transformation into parameters suitable for rpc layer and conversion back into expected type or exception
     * @param methodName
     * @param responseType
     * @param params
     * @return
     * @throws SystemException
     */
    public Object executeRmiMethod(String serializationFormatId, Class<?> responseClass, String serviceName, String methodName, long timeout, Object... parameters) throws Exception
    {
        List<byte[]> paramList = new ArrayList<byte[]>();
        paramList.add(serviceName.getBytes());
        paramList.add(methodName.getBytes());
        paramList.add(serializationFormatId.getBytes());
        
        NimrodObjectSerializationInterface serializer = NimrodObjectSerializer.GetInstance().getSerializers().get(serializationFormatId);
        if(serializer == null) {
            throw new NimrodSerializerNotFoundException();
        }
            
        for(Object param : parameters)
        {
            paramList.add(serializer.serialize(param));
        }
        try {
            List<byte[]> responseList = sendAndReceive(paramList, timeout);
            Object response = null;
            if(responseList.size() > 0)
            {
                try
                {
                    response = serializer.deserialize(responseList.get(0), responseClass);
                    return response;
                }
                catch (NimrodSerializationException e)
                {
                    throw e;
                }
            } else {
                throw new NimrodRmiException();
            }
            
        } catch (NimrodRmiRemoteException r) {
            Exception responseException = null;
            Class exceptionClass = null;
            //Try and deserialize the bytes describing the actual remote exception contained in the NimrodRmiRemoteException
            try {
                exceptionClass = Class.forName(r.getRemoteExceptionName());
                responseException = (Exception)serializer.deserialize(r.getRemoteExceptionAsBytes(),exceptionClass );
                
            } catch (Throwable t) {
                //If that fails try instantiation the class based on the name
                if(exceptionClass != null) {
                    responseException = (Exception)exceptionClass.newInstance();
                }
            }
            if(responseException == null) {
                //If all that fails throw the original NimrodRmiRemoteException
               throw r;
            } else {
                throw  responseException;   
            }
        } catch (NimrodRmiNotConnectedException nce) {
            logger.error("executeRmiMethod: Calling " + serviceName + ":" + methodName+" but not connected.");
            throw nce;
        } catch (Throwable t) {
            if(t instanceof Exception)
                throw (Exception)t;
            else
                throw new Exception(t);
        }
    }


    /**
     * Internal method for doing actual RMI over zeroMQ
     */
    private List<byte[]> sendAndReceive(List<byte[]> parameters, long timeout) throws NimrodRmiException

    {
        // Add in a unique thread request identifier
        String threadRequestIdentifier = Thread.currentThread().getName() + "," + threadRequestId.getAndIncrement();

        if (threadRequestIdentifier.startsWith(heartBeatThreadName) == false && connected == false) {
            NimrodRmiNotConnectedException e = new NimrodRmiNotConnectedException();
            throw e;
        }
        InprocConnection inprocConnection = null;
        int poolRetryCount = 0;
        try {
            while (inprocConnection == null && poolRetryCount < inprocPoolSize) {
                if (inprocPool != null) {
                    inprocConnection = (InprocConnection) inprocPool.borrowObject();
                    if (connected == false) {
                        inprocPool.invalidateObject(inprocConnection);
                        NimrodRmiNotConnectedException e = new NimrodRmiNotConnectedException();
                        throw e;
                    }
                    inprocConnection.alreadyReturnedToPool = false;
                    if (inprocConnection.isAlive == false) {
                        logger.info("Invalidate");
                        inprocPool.invalidateObject(inprocConnection);
                        inprocConnection = null;
                        poolRetryCount++;
                    }
                } else {
                    // Pool is not ready so sleep a bit...this will only occur
                    // during startup and on disconnect/reconnect
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                    }
                }
            }
            if (poolRetryCount == inprocPoolSize) {
                NimrodRmiNotConnectedException e = new NimrodRmiNotConnectedException();
                throw e;
            }
            inprocConnection.timeout = timeout;
            inprocConnection.threadRequestIdentifier = threadRequestIdentifier;
            callsInProgress.put(inprocConnection.thread.getName(), inprocConnection);

            // Insert the current calling thread name as the first parameter
            List<byte[]> actualParameters = new ArrayList<byte[]>(1 + parameters.size());
            actualParameters.add(threadRequestIdentifier.getBytes());
            actualParameters.addAll(parameters);

            // Initiate the sending process thru the InprocConnection got from
            // the pool..
            inprocConnection.queueIn.put(actualParameters);
            // Get the response from the InprocConnection
            List<byte[]> response = inprocConnection.queueOut.take();
            // if(inprocConnection.alreadyReturnedToPool == false)
            // inprocPool.returnObject(inprocConnection);
            // inprocConnection = null;
            if (response.size() == 0) {
                // Its a timeout indicated by nothing in response
                throw new NimrodRmiTimeoutException("timeout");
            }
            // See if the response is actually an exception in which case
            // 'create' exception and throw...can't do this directly here so
            // propagate back data wrapped in TransportException
            if ("0".equals(new String(response.get(0)))) {
                // the second entry contains the exception class name..
                // This needs more work to propogate back correct
                // exception...just a generic one for now with Name or
                // description..
                String className = new String(response.get(1));
                byte[] classAsBytes = new byte[] { 0 };
                if (LOST_CONNECTION.equals(className)) {
                    
                    throw new NimrodRmiNotConnectedException("Lost Connection in " + inprocConnection.name + " while call in progress");
                }
                if (response.size() > 2)
                    classAsBytes = response.get(2);
                throw new NimrodRmiRemoteException("NimrodRmiRemoteException ClassName=" + className, className, classAsBytes);
            }
            // Its a good response to remove the 0/1 indicator in the first
            // entry and return the rest back to caller
            response.remove(0);
            return response;
        } catch (Exception e) {
            if (e instanceof NimrodRmiException == false)
                throw new NimrodRmiException("Unexpected Exception", e);
            else
                throw (NimrodRmiException) e;
        } finally {
            if (inprocConnection != null && inprocConnection.alreadyReturnedToPool == false) {
                try {
                    inprocConnection.queueIn.clear();
                    inprocConnection.queueOut.clear();
                    inprocPool.returnObject(inprocConnection);
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * This is the task that the InprocPool is effectively starting and managing
     * 
     * @author andy
     *
     */
    class SendAndReceiveTask implements Runnable {
        InprocConnection inprocConnection;
        boolean keepRunning = true;
        boolean keepRunningInnerLoop = true;

        SendAndReceiveTask(InprocConnection inprocConnection) throws Exception {
            try {
                callsInProgressLock.lock();
                if (connected == false) {
                    // logger.info("InprocConnection: SendAndReceiveTask cannot be created at this time");
                    throw new Exception("SendAndReceiveTask cannot be created at this time");
                }
                this.inprocConnection = inprocConnection;
            } finally {
                callsInProgressLock.unlock();
            }
        }

        void shutdown() {
            keepRunning = false;
            try {
                logger.info("InprocConnection: SendAndReceiveTask shutdown");
                if (inprocConnection.queueIn.size() < 2)
                    inprocConnection.queueIn.put(new ArrayList<byte[]>());
            } catch (InterruptedException e) {

            }
        }

        @Override
        public void run() {
            inprocConnection.lock.lock();
            long id = inprocThreadId.getAndIncrement();
            String identity = INPROC_PREFIX+getInstanceName()+"-" + thisInstanceId + "-" + id;
            Thread.currentThread().setName(identity);
            inprocConnection.name = identity;
            // logger.info("Started thread "+identity);
            int retry = 3;

            // Create the socket end-point
            while (retry >= 0) {
                try {
                    inprocConnection.socket = context.socket(ZMQ.REQ);
                    break;
                } catch (Exception e2) {
                    // Need better way of distinguishing this exception
                    if (e2.getMessage().contains("Too many open files")) {
                        if (retry == 0) {
                            inprocConnection.isAlive = false;
                            logger.error("Transport unavailable : Too many open files. Retry count = " + RETRY);
                            break;
                        } else {
                            // overallRetryCount++;
                            try {
                                // logger.info("Too many open files : retry "+retry);
                                retry--;
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                ;
                            }
                        }
                    } else {
                        inprocConnection.isAlive = false;
                        logger.error("Unexpected transport exception ", e2);
                        break;
                    }
                }
            }
            // If the socket was unable to be created then terminate thread now
            if (inprocConnection.isAlive == false) {
                logger.info("Thread " + inprocConnection.name + " terminated...unable to create inproc socket.");
                return;
            }

            inprocConnection.socket.setLinger(0);
            inprocConnection.socket.setIdentity(identity.getBytes());
            inprocConnection.socket.connect(internalSocketName);

            // Tell the thread waiting for this new InprocConnection and
            // associated task that its ready...
            inprocConnection.condition.signal();
            inprocConnection.lock.unlock();
            logger.info("thread " + identity + " ready");
            // Stay in loop for duration of this process..
            while (keepRunning && keepRunningInnerLoop) {
                try {
                    // This will block waiting for requests
                    List<byte[]> parameters = inprocConnection.queueIn.take();
                    // Empty means time to exit loop and shutdown
                    if (parameters.size() == 0) {
                        logger.info("inprocConnection.queueIn.take empty so break..");
                        break;
                    }
                    // Remember the threadRequestId so that we can check the
                    // response...because a previous (timedout) response for
                    // this pooled inprocconnection might arrive and mess things
                    // up
                    String threadRequestId = new String(parameters.get(0));

                    int timeout = (int) inprocConnection.timeout;
                    // Check if socket needs recreating..

                    inprocConnection.socket.setReceiveTimeOut(timeout);

                    for (int i = 0; i < parameters.size(); i++) {
                        inprocConnection.socket.send(parameters.get(i), (i == parameters.size() - 1) ? 0 : ZMQ.SNDMORE);
                    }
                    List<byte[]> response = new ArrayList<byte[]>();
                    // Receive the initial response..first frame will contain
                    // the original threadRequestId
                    while (keepRunningInnerLoop) {
                        byte[] frame1;
                        byte[] frame2;
                        try {
                            frame1 = inprocConnection.socket.recv(0);
                        } catch (Throwable e) {
                            // Must be a timeout
                            logger.error("Unexpected exception on recv", e);
                            if (inprocConnection.queueOut.peek() == null)
                                inprocConnection.queueOut.put(response);
                            continue;
                        }
                        if (frame1 == null) {
                            logger.error("frame1 == null .. treat as a timeout");
                            if (inprocConnection.queueOut.peek() == null)
                                inprocConnection.queueOut.put(response);
                            else {
                                Object o = inprocConnection.queueOut.peek();
                                if (o instanceof byte[] == false)
                                    logger.error("extra info : inprocConnection.queueOut is not empty?? - contains object " + o.toString());
                                else
                                    logger.error("extra info : inprocConnection.queueOut is not empty?? - contains byte[] " + o.toString());
                            }
                            continue;
                        }

                        // Check that this response has same request id as the
                        // one just sent..this will be in response.get(0)
                        // Its possible that a previous 'timeout' response has
                        // just arrived..this will have an earlier requestid
                        // If it does then need to flush through this response
                        // and continue to wait for actual...
                        String replyThreadRequestId = new String(frame1);
                        if (replyThreadRequestId.equals(threadRequestId) == false) {
                            // Read/Flush all frames on the response and go back
                            // to recv..hopefully we will get the real response
                            // next
                            logger.error("Expecting threadRequestId=" + threadRequestId + " but received replyThreadRequestId=" + replyThreadRequestId);
                            while (inprocConnection.socket.hasReceiveMore()) {
                                byte[] nextFrame = inprocConnection.socket.recv(0);
                                String nextFrameStr = new String(nextFrame);
                                if (LOST_CONNECTION.equals(nextFrameStr)) {
                                    keepRunningInnerLoop = false;
                                    throw new Exception("Received LOST_CONNECTION...terminate inprocConnection.");
                                }
                            }
                            continue;
                        }
                        // Its the right response to the right request..
                        break;
                    }
                    // The rest of the frames constitute the response
                    while (inprocConnection.socket.hasReceiveMore()) {
                        response.add(inprocConnection.socket.recv(0));
                    }
                    inprocConnection.queueOut.put(response);
                    // If that was a message saying lost connection to server
                    // then this thread should terminate...
                    if (response.size() == 2) {
                        String frame1 = new String(response.get(0));
                        if ("0".equals(frame1)) {
                            String frame2 = new String(response.get(1));
                            if (LOST_CONNECTION.equals(frame2)) {
                                logger.error("Received LOST CONNECTION in SendAndReceiveTask");
                                // break;
                            }
                        }
                    } else if (response.size() == 0) {
                        logger.error("response.size() == 0 ...which is going to be intrepreted as a timeout!!");
                    }

                } catch (Exception e) {
                    logger.error("Exception occurred in SendAndReceiveTask", e);
                } finally {
                    synchronized (callsInProgress) {
                        if (callsInProgress.remove(inprocConnection.thread.getName())==null){
                            logger.warn("callsInProgress.remove("+inprocConnection.thread.getName()+") did not remove anything, size="+callsInProgress.size());
                        }
                      
                        if (connected == false && callsInProgress.size() == 0) {
                            callsInProgressLock.lock();
                            callsInProgressCondition.signal();
                            callsInProgressLock.unlock();
                        }
                    }
                }
                // Another way of stopping this thread ..keepRunningInnerLoop
                // will have be set to false if LOST_CONNECTION jumps in front
                // of expected message
                if (keepRunningInnerLoop == false) {
                    break;
                }
            }
            // Loop finished so thread is due to finish so dispose socket
            inprocConnection.socket.close();
            inprocConnection.socket = null;
            inprocConnection.alreadyReturnedToPool = true;
            try {
                if (inprocPool != null)
                    inprocPool.invalidateObject(inprocConnection);
            } catch (Exception e) {
                logger.error("Exception freeing inprocConnection", e);
            }
            logger.info("Thread " + inprocConnection.name + " terminated...InprocSocket closed");
        }

    }

    /**
     * THIS CAN ONLY BE CALLED BY QueueTask thread ... its the only thread that
     * can talk to frontend socket. Send a response on frontEnd socket for all
     * calls in progress i.e. send an Exception message. The calls in progress
     * are identified by looking in the Pool of InprocConnections..
     */
    private void interruptCallsInProgress() {
        logger.info("interruptCallsInProgress start");
        int count = 0;
        try {
            callsInProgressLock.lock();
            Iterator<InprocConnection> inProcs = callsInProgress.values().iterator();
            while (inProcs.hasNext()) {
                // Send an Exception message to the waiting threads
                InprocConnection inProc = inProcs.next();
                frontend.send(inProc.thread.getName().getBytes(), ZMQ.SNDMORE);
                frontend.send("".getBytes(), ZMQ.SNDMORE);
                frontend.send(inProc.threadRequestIdentifier.getBytes(), ZMQ.SNDMORE);
                frontend.send("0".getBytes(), ZMQ.SNDMORE);
                frontend.send(LOST_CONNECTION.getBytes(), 0);
                count++;
            }
            try {
                callsInProgressCondition.await(10, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e1) {
            }
        } finally {
            callsInProgressLock.unlock();
        }
        try {
            for (InprocConnection inprocConnection : currentInprocConnections) {
                inprocConnection.task.shutdown();
                inprocPool.invalidateObject(inprocConnection);
            }
            currentInprocConnections.clear();
            if (inprocPool != null) {
                inprocPool.clear();
                inprocPool.close();
                inprocPool = null;
            }
        } catch (Exception e) {
            logger.error("Closing InprocPool", e);
        }
        logger.info("interruptCallsInProgress finish, " + count + " calls interrupted");
    }

    private void initializeHeartbeat() {
        // Start Heartbeat thread to detect disconnects and initiate reconnect
        // to next server
        try {
            heartbeatTask = new HeartbeatTask(queueHandler);
            heartbeatThread = new Thread(heartbeatTask, heartBeatThreadName);
            if (isFirstTime)
                heartbeatTask.lock.lock();
            heartbeatThread.start();
            if (isFirstTime)
                heartbeatTask.condition.await();
        } catch (Exception e) {
            logger.error("Exception starting heartbeat");
        } finally {
            if (heartbeatTask.lock.isHeldByCurrentThread())
                heartbeatTask.lock.unlock();
        }

    }

    class HeartbeatTask implements Runnable {
        QueueTask queue;
        boolean keepRunning = true;
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        public HeartbeatTask(QueueTask queue) {
            this.queue = queue;
            connected = false;
        }

        public void stop() {
            keepRunning = false;
            heartbeatThread.interrupt();

        }

        public void run() {
            int logCnt = 0;
            if (isFirstTime) {
                logger.info(" HeartbeatThread aquire lock");
                lock.lock();
            }
            try {
                logger.info(" HeartbeatThread starting");
                long time;
                boolean firstLoop = true;
                while (!Thread.currentThread().isInterrupted() && keepRunning) {
                    try {
                        if (firstLoop == false) {
                            try {
                                Thread.sleep(TIMEOUT);
                            } catch (InterruptedException e) {
                                // Sleep has been interrupted so must be a
                                // shutdown of heartbeat..
                                if (keepRunning == false)
                                    continue;
                            }
                        } else
                            firstLoop = false;

                        time = System.nanoTime();
                        // sendAndReceiveHeartbeat will throw a TimeoutException
                        byte[] response = sendAndReceiveHeartbeat(Long.toString(time).getBytes(), TIMEOUT);
                        if (connected == false) {
                            connected = true;
                            logCnt = 0;
                            if (inprocPool == null) {
                                inprocThreadId.set(1);
                                inprocPool = new GenericObjectPool(new PoolConnectionFactory(context), inprocPoolSize, GenericObjectPool.WHEN_EXHAUSTED_BLOCK, -1);
                            }
                            // logger.info("Connection established");
                            notifyConnectionEstablished();
                            if (isFirstTime) {
                                condition.signal();
                                lock.unlock();
                            }
                        }
                        if (connectedMadeAtDateTime == null)
                            connectedMadeAtDateTime = new Date();
                    } catch (Exception e) {
                        if (e instanceof NimrodRmiTimeoutException == false && stopped == false) {
                            // Unexpected..so print full trace
                            logger.info(" Got an unexpected exception - trace is : ", e);
                        }
                        boolean wasConnected = connected;
                        connected = false;
                        // If running with multiple externalSocketURL then
                        // attempt to failover to next one..this is not fully
                        // tested/implemented currently
                        if (externalSocketURL.size() > 1) {
                            try {
                                queue.stopQueue();
                            } catch (ZMQException zmqe) {
                                if (stopped == false) {
                                    logger.info("Exception whilst stopping Queue", zmqe);
                                }
                            }
                            heartbeatThread = null;
                            connectedMadeAtDateTime = null;
                            if (stopped == false) {
                                if (logCnt++ % 10 == 0)
                                    logger.info(" Got an exception on HEARTBEAT response on " + externalSocketURL.get(currentExternalSocketEntry) + " initiating reconnect");
                                // Spawn a thread to notify listens that there
                                // has been a break in connection
                                if (wasConnected)
                                    notifyBreakInConnection();

                                if (isFirstTime) {
                                    condition.signal();
                                    lock.unlock();
                                }
                                try {
                                    initialize();
                                } catch (Exception e1) {
                                    // TODO Decide what to do here ?????
                                    logger.error("Exception calling initialize", e);
                                }
                            }
                            break;
                        } else {
                            // When there is only one connection then report
                            // heartbeat problem..but continue as is...
                            connectedMadeAtDateTime = null;
                            if (stopped == false) {
                                if (logCnt++ % 10 == 0)
                                    logger.info("Connection to Server NOT detected on channel " + externalSocketURL.get(currentExternalSocketEntry) + "..will retry.");
                                // NEED TO SEND MESSAGE TO QueueTask to tell it
                                // to reply to any/all callsInProgress ..and
                                // dispose and recreate backend socket to flush
                                // messages
                                // Send a message into the queue pump to tell it
                                // to reply to all waiting inproc threads
                                try {
                                    callsInProgressLock.lock();
                                    Socket socket = context.socket(ZMQ.REQ);
                                    socket.setLinger(0);
                                    socket.setIdentity(INTERRUPT_CALLS_IN_PROGRESS_TASKNAME.getBytes());
                                    socket.connect(internalSocketName);
                                    if (wasConnected)
                                        socket.send(INTERRUPT_CALLS_IN_PROGRESS_OPERATION, 0);
                                    else
                                        socket.send(RESET_OPERATION, 0);
                                    callsInProgressCondition.await(10, TimeUnit.MILLISECONDS);
                                    socket.close();
                                } catch (Exception ee) {
                                    logger.error("Trying to interrupt calls in progress", e);
                                } finally {
                                    callsInProgressLock.unlock();
                                }
                                if (wasConnected)
                                    notifyBreakInConnection();
                                if (isFirstTime) {
                                    condition.signal();
                                    lock.unlock();
                                }
                            }
                            // Just continue loop...
                        }
                    }
                }
                logger.info("HeartbeatThread terminated");
            } finally {
                if (isFirstTime) {
                    if (lock.isHeldByCurrentThread())
                        lock.unlock();
                }
            }
        }
    }

    /**
     * Need dedicated method for heartbeat because cannot go thru pooling
     * mechanism.. Pool might be exhausted and blocking can then occur..which
     * means we will never progress..
     * 
     * @param parameter
     * @param timeout
     * @param aSessionId
     * @return
     * @throws NimrodRmiTransportException
     */
    private byte[] sendAndReceiveHeartbeat(byte[] parameter, long timeout) throws NimrodRmiException {
        String identity = null;
        identity = Thread.currentThread().getName();
        Socket client = null;
        int retry = 3;
        while (retry >= 0) {
            try {
                client = context.socket(ZMQ.REQ);
                break;
            } catch (Exception e2) {
                // Need better way of distinguishing this exception
                if (e2.getMessage().contains("Too many open files")) {
                    if (retry == 0)
                        throw new NimrodRmiTransportException("Transport unavailable : Too many open files");
                    else {
                        try {
                            retry--;
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            ;
                        }
                    }
                } else
                    throw new NimrodRmiTransportException("Unexpected transport exception", e2);
            }
        }
        // Set the identity of the endpoint of this socket to something unique
        // to this process so the answer gets routed back correctly
        client.setIdentity((identity + seqNo.getAndIncrement()).getBytes());
        client.connect(internalSocketName);
        if (timeout != -1)
            client.setReceiveTimeOut((int) timeout);
        client.setLinger(0);
        client.send(identity.getBytes(), ZMQ.SNDMORE);
        client.send(parameter, 0);
        // Receive the initial response..first frame will contain a 0
        // (error/exception) or 1 (good)
        byte[] frame1;
        byte[] frame2;
        try {
            frame1 = client.recv(0);
        } catch (Throwable e) {
            // Unexpected Exception
            logger.error("Unexpected error from recv", e);
            try {
                client.close();
            } catch (Exception e1) {
                logger.error("Unexpected error from recv - trying to close socket", e1);
            }
            throw new NimrodRmiTransportException("Unexpected error from recv - trying to close socket", e);
        }
        // If first frame is null then its a timeout
        if (frame1 == null) {
            try {
                client.close();
            } catch (Exception e1) {
                logger.error("Timeout on recv - error trying to close socket", e1);
            }
            throw new NimrodRmiTimeoutException("timeout");
        }
        // logger.info("Something back");
        // Test the first frame..if its a 0 then the second frame contains an
        // exception name
        if ("0".equals(new String(frame1))) {
            frame2 = client.recv(0);
            client.close();
            // This needs more work to propogate back correct exception...just a
            // generic one for now with Name or description..
            throw new NimrodRmiTransportException("RemoteExceptionClassName=" + new String(frame2));
        }
        // There is only one frame response
        byte[] response = null;
        while (client.hasReceiveMore()) {
            response = client.recv(0);
        }
        client.close();
        return response;
    }

    private void notifyConnectionEstablished() {
        logger.info("Connection to Server on channel " + externalSocketURL.get(currentExternalSocketEntry) + " ESTABLISHED");
        alreadyNotifiedBreak = false;

        if (eventListeners.size() == 0)
            return;
        Thread thread = new Thread("notifyConnectionEstablished") {
            public void run() {
                for (NimrodRmiEventListener listener : eventListeners) {
                    listener.onConnectionEstablished(instance);
                }
            }
        };
        thread.start();

        breakTolerance = BREAKTOLERANCECOUNT;
    }

    private void notifyBreakInConnection() {
        if (eventListeners.size() == 0)
            return;
        // Only do this if exceeds tolerance count...don't want to trigger lots
        // of activity if its a temporary break
        breakTolerance--;
        if (breakTolerance == 0) {
            breakTolerance = BREAKTOLERANCECOUNT;
            // Only do this once per break
            if (alreadyNotifiedBreak == true) {
                return;
            }
            Thread thread = new Thread("notifyBreakInConnection") {
                public void run() {
                    for (NimrodRmiEventListener listener : eventListeners) {
                        listener.onBreakInConnection(instance);
                    }
                }
            };
            thread.start();
            alreadyNotifiedBreak = true;
        }
    }

}