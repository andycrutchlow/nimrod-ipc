package com.nimrodtechs.ipc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.nimrodtechs.exceptions.NimrodPubSubException;
import com.nimrodtechs.serialization.NimrodObjectSerializationInterface;
import com.nimrodtechs.serialization.NimrodObjectSerializer;

public abstract class ZeroMQCommon {
	private static Logger logger = LoggerFactory.getLogger(ZeroMQCommon.class);

	protected static Context context;
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
	
	protected static final String AGENT_SUBJECT_PREFIX = "zmq.agent.";
	
	protected static final byte[] ZERO_AS_BYTES = "0".getBytes();
	protected static final byte[] ONE_AS_BYTES = "1".getBytes();
	protected static final byte[] EMPTY_FRAME = "".getBytes();
    protected final static byte[] SHUTDOWN_OPERATION = "~0".getBytes();
	protected static final byte[] READY_OPERATION = "~1".getBytes();
	protected final static byte[] INTERRUPT_CALLS_IN_PROGRESS_OPERATION = "~2".getBytes();
	protected static final byte[] RESET_OPERATION = "~3".getBytes();
	
	private static Map<String ,ZeroMQCommon> instances = new HashMap<String ,ZeroMQCommon>();
	
	protected static int TIMEOUT = 2000;
	protected static int RETRY = 3;
	protected List<String> externalSocketURL = new ArrayList<String>();
	protected int currentExternalSocketEntry = 0;

	/**
	 * In the context of a rmi client/server then server socket is the listening
	 * side that clients connect to and subsequently send requests in on.
	 * In the context of pubsub then server socket is the socket that messages will be
	 * published on and that subscribers connect to to receive messages on. 
	 */
	protected String serverSocket = null;
	protected String serverSocketLongVersion = null;
	protected String clientSocket = null;
	protected  Socket frontend;

	protected  Socket backend;
	
	protected NimrodObjectSerializationInterface defaultSerializer;
	protected static String defaultSerializerId;
	
	protected String instanceName = null;
	public String getInstanceName() {
	    if(instanceName == null)
	        instanceName = getDefaultInstanceName();
        return instanceName;
    }
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
        instances.put(instanceName, this);
    }
    protected static ZeroMQCommon getInstance(String name)
    {
        return instances.get(name);
    }
    protected abstract String getDefaultInstanceName();
    
    /**
     * manyToOne = true means the socket will be written to by many (this process is one of them) and read by one (another process).
     */
    protected boolean manyToOne = false;
    public void setManyToOne(boolean manyToOne) throws Exception {
        if(this instanceof ZeroMQPubSubSubscriber == false && this instanceof ZeroMQPubSubPublisher == false)
            throw new NimrodPubSubException("setManyToOne only applicable for ZeroMQPubSubSubscriber or ZeroMQPubSubPublisher");
        this.manyToOne = manyToOne;
    }
    
    protected static String zeroMQAgentInboundSocketUrl = System.getProperty("zeroMQAgentInboundSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/zeroMQAgentInboundSocketUrl.pubsub");
	public static String getZeroMQAgentInboundSocketUrl() {
        return zeroMQAgentInboundSocketUrl;
    }
    public static void setZeroMQAgentInboundSocketUrl(String zeroMQAgentInboundSocketUrl) {
        ZeroMQCommon.zeroMQAgentInboundSocketUrl = zeroMQAgentInboundSocketUrl;
    }
    protected static String zeroMQAgentOutboundSocketUrl = System.getProperty("zeroMQAgentOutboundSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/zeroMQAgentOutboundSocketUrl.pubsub");
    public static String getZeroMQAgentOutboundSocketUrl() {
        return zeroMQAgentOutboundSocketUrl;
    }
    public static void setZeroMQAgentOutboundSocketUrl(String zeroMQAgentOutboundSocketUrl) {
        ZeroMQCommon.zeroMQAgentOutboundSocketUrl = zeroMQAgentOutboundSocketUrl;
    }
    private ZeroMQPubSubPublisher agentPublisher;
    
    public void initialize() throws Exception {
		if(context != null) {
		    logger.info("Already initialized - ignored.");
		}
		//If externalSocketURL's have not already been injected then see if available on command line
		if(serverSocket == null) {
		    String sockName = System.getProperty("nimrod.rpc.serverRmiSocket");
			if(sockName == null){
				throw new Exception("Missing serverRmiSocket configuration");
			} else {
			    setServerSocket(sockName);
			}
			
		}
		//TODO This needs a bit more work..if there are multiples then need to pass in which is default
		for(Map.Entry<String, NimrodObjectSerializationInterface> entry : NimrodObjectSerializer.GetInstance().getSerializers().entrySet()) {
		    defaultSerializer = entry.getValue();
		    defaultSerializerId = entry.getKey();
		    break;
		}
		if(defaultSerializer == null) {
		    throw new Exception("No NimrodObjectSerializers available. Atleast 1 must be configured.");
		}
		context = ZMQ.context(1);
		logger.info("created ZMQ context Version : "+ZMQ.getFullVersion());
	}
	
    public void setServerSocket(String sockName) {
        if (sockName != null) {
            clientSocket = sockName;
            if (sockName.startsWith("tcp://") && manyToOne == false) {
                String s1 = sockName.replace("tcp://", "");
                String[] s2 = s1.split(":");
                if (s2.length > 1) {
                    if (s2[0].equals("*")) {
                        try {
                            serverSocketLongVersion = "tcp://" + InetAddress.getLocalHost().getHostName() + ":" + s2[1];
                            serverSocket = sockName;
                        } catch (UnknownHostException e) {
                            this.serverSocket = sockName;
                        }
                    } else {
                        serverSocketLongVersion = sockName;
                        serverSocket = "tcp://*:" + s2[1];
                    }
                } else {
                    this.serverSocket = sockName;
                }
            } else {
                this.serverSocket = sockName;
            }
            if (serverSocketLongVersion == null)
                serverSocketLongVersion = serverSocket;
        }
    }

    public String getServerSocket() {
        return serverSocket;
    }
	
    /**
     * A method to check equality of first 2 bytes.
     * Good enough to recognize all of the internal activities.
     * @param lhs
     * @param rhs
     * @return
     */
    protected boolean testHighOrderBytesAreEqual(byte[] lhs, byte[] rhs)
    {
        if(lhs.length < 2 || rhs.length < 2)
            return false;
        if(lhs[0] == rhs[0] && lhs[1] == rhs[1])
            return true;
        else
            return false;
    }
    protected void initializeAgentSubscriber() {
//        agentPublisher = new ZeroMQPubSubPublisher();
//        agentPublisher.setServerSocket(getZeroMQAgentOutboundSocketUrl());
//        agentPublisher.setInstanceName("agentOut");
//        try {
//            agentPublisher.setManyToOne(true);
//            agentPublisher.initialize();
//        } catch (Exception e) {
//            logger.error("initializeAgentOutboundSocket : failed",e);
//        }
    }
    
    protected void initializeAgentPublisher() {
        agentPublisher = new ZeroMQPubSubPublisher();
        agentPublisher.setServerSocket(getZeroMQAgentInboundSocketUrl());
        agentPublisher.setInstanceName("agentPublisher");
        try {
            agentPublisher.setManyToOne(true);
            agentPublisher.initialize();
        } catch (Exception e) {
            logger.error("initializeAgentInboundSocket : failed",e);
        }
    }
    
    protected void agentPublish(String subject, String message) {
        agentPublisher.publish(subject, message);
    }
    
    
}
