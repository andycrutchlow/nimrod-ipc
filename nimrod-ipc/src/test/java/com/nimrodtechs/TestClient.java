package com.nimrodtechs;

import ch.qos.logback.classic.Logger;

import com.nimrodtechs.exceptions.NimrodRmiNotConnectedException;
import com.nimrodtechs.rmi.zmq.ZeroMQRmiClient;
import com.nimrodtechs.serialization.NimrodObjectSerializer;
import com.nimrodtechs.serialization.kryo.KryoSerializer;

public class TestClient {
    static ZeroMQRmiClient testServerConnection;
    
	public static void main(String[] args) {
	  //Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if(testServerConnection != null)
                    testServerConnection.dispose();
            }
        });

        //Configure the general serializer by adding a kryo serializer
        NimrodObjectSerializer.GetInstance().getSerializers().put("kryo",new KryoSerializer());
        testServerConnection = new ZeroMQRmiClient();
        testServerConnection.setZmqClientName("TestServerConnection");
        testServerConnection.setServerRmiSocket(System.getProperty("rmiServerSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/rmiServerSocket"));
        try {
            testServerConnection.initialize();
            while(true) {
                try {
                    String response = (String)testServerConnection.executeRmiMethod(String.class, "ANDYTEST","rmiTestMethod1", "hello");
                    System.out.println("response = "+response);
                } catch (NimrodRmiNotConnectedException e) {
                    System.out.println("Server not detected .. try again");
                }
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        testServerConnection.dispose();
	}

}
