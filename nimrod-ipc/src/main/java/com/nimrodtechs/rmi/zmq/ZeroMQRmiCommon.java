package com.nimrodtechs.rmi.zmq;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.nimrodtechs.serialization.NimrodObjectSerializationInterface;
import com.nimrodtechs.serialization.NimrodObjectSerializer;

public class ZeroMQRmiCommon {
	private static Logger logger = LoggerFactory.getLogger("ZeroMQRmiCommon");

	protected static Context context;
	protected final static String INTERNAL_SOCKET_NAME_PREFIX = "inproc://inproc";
	protected final static String SHUTDOWN_TASKNAME = "shutdowntask";
	protected final static String INTERRUPT_CALLS_IN_PROGRESS_TASKNAME = "interruptcallsinprogresstask";

	protected static final String LOST_CONNECTION = "LostConnectionToServer";
	protected static final String PUMP_PREFIX = "zmqPump-";
	protected static final String HEARTBEAT_PREFIX = "zmqHrtb-";
	protected static final String WORKER_PREFIX = "zmqWorker-";
	
	
	protected static final byte[] ZERO_AS_BYTES = "0".getBytes();
	protected static final byte[] ONE_AS_BYTES = "1".getBytes();
	protected static final byte[] EMPTY_FRAME = "".getBytes();
    protected final static byte[] SHUTDOWN_OPERATION = "~0".getBytes();
	protected static final byte[] READY_OPERATION = "~1".getBytes();
	protected final static byte[] INTERRUPT_CALLS_IN_PROGRESS_OPERATION = "~2".getBytes();
	protected static final byte[] RESET_OPERATION = "~3".getBytes();
	
	
	protected static int TIMEOUT = 2000;
	protected static int RETRY = 3;
	protected List<String> externalSocketURL = new ArrayList<String>();
	protected int currentExternalSocketEntry = 0;

	protected String serverRmiSocket = null;
	protected String serverRmiSocketLongVersion = null;
	protected String clientRmiSocket = null;
	protected  Socket frontend;

	protected  Socket backend;
	
	protected NimrodObjectSerializationInterface defaultSerializer;
	protected String defaultSerializerId;
	
	public void initialize() throws Exception {
		if(context != null) {
			throw new Exception("Already initialized");
		}
		//If externalSocketURL's have not already been injected then see if available on command line
		if(serverRmiSocket == null) {
		    String sockName = System.getProperty("nimrod.rpc.serverRmiSocket");
			if(sockName == null){
				throw new Exception("Missing serverRmiSocket configuration");
			} else {
			    setServerRmiSocket(sockName);
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
    public void setServerRmiSocket(String sockName) {
        if (sockName != null) {
            clientRmiSocket = sockName;
            if (sockName.startsWith("tcp://")) {
                String s1 = sockName.replace("tcp://", "");
                String[] s2 = s1.split(":");
                if (s2.length > 1) {
                    if (s2[0].equals("*")) {
                        try {
                            serverRmiSocketLongVersion = "tcp://" + InetAddress.getLocalHost().getHostName() + ":" + s2[1];
                            serverRmiSocket = sockName;
                        } catch (UnknownHostException e) {
                            this.serverRmiSocket = sockName;
                        }
                    } else {
                        serverRmiSocketLongVersion = sockName;
                        serverRmiSocket = "tcp://*:" + s2[1];
                    }
                } else {
                    this.serverRmiSocket = sockName;
                }
            } else {
                this.serverRmiSocket = sockName;
            }
            if (serverRmiSocketLongVersion == null)
                serverRmiSocketLongVersion = serverRmiSocket;
        }
    }

    public String getServerRmiListenSocket() {
        return serverRmiSocket;
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
}
