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

package com.nimrodtechs;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nimrodtechs.exceptions.NimrodRmiNotConnectedException;
import com.nimrodtechs.ipc.ZeroMQRmiClient;
import com.nimrodtechs.serialization.NimrodObjectSerializer;
import com.nimrodtechs.serialization.kryo.KryoSerializer;

public class TestClientMultithread {
    final static Logger logger = LoggerFactory.getLogger(TestClientMultithread.class);
    static ZeroMQRmiClient testServerConnection;
    static boolean keepRunning = true;
    static BigDecimal bd = new BigDecimal("100.123");
	public static void main(String[] args) {
	  //Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                keepRunning = false;
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if(testServerConnection != null)
                    testServerConnection.dispose();
            }
        });
        new TestClientMultithread().test();
	}
	
	void test() {
        //Configure the general serializer by adding a kryo serializer
        NimrodObjectSerializer.GetInstance().getSerializers().put("kryo",new KryoSerializer());
        testServerConnection = new ZeroMQRmiClient();
        testServerConnection.setInstanceName("TestServerConnection");
        testServerConnection.setServerSocket(System.getProperty("rmiServerSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/rmiServerSocket"));
        try {
            testServerConnection.initialize();
            //kick of 10 threads running rmi test calling remote methods
            for(int i=0;i<10;i++) {
                
                new Thread(new TestTask()).start();
            }
            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	}
	
	AtomicInteger count = new AtomicInteger(1);
	
	class TestTask implements Runnable {
	    public void run() {
            try {
                Thread.currentThread().setName("TestClientThread-"+count.getAndIncrement());
                while(keepRunning) {
                    try {
                        String response = (String)testServerConnection.executeRmiMethod(String.class, "ANDYTEST","rmiTestMethod1", "hello1 from "+Thread.currentThread().getName());
                        logger.info("response = "+response);
//                        String response2 = (String)testServerConnection.executeRmiMethod(String.class, "ANDYTEST","rmiTestMethod2", "hello2 from "+Thread.currentThread().getName(), bd, "there" );
//                        logger.info("response2 = "+response2);
                    } catch (NimrodRmiNotConnectedException e) {
                        System.out.println("Server not detected .. try again");
                    }
                    Thread.sleep(2000);
                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
	    }
	}
}
