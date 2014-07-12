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

import ch.qos.logback.classic.Logger;

import com.nimrodtechs.exceptions.NimrodRmiNotConnectedException;
import com.nimrodtechs.ipc.ZeroMQRmiClient;
import com.nimrodtechs.serialization.NimrodObjectSerializer;
import com.nimrodtechs.serialization.kryo.KryoSerializer;

public class TestClient {
    static ZeroMQRmiClient testServerConnection;
    static boolean keepRunning = true;
	public static void main(String[] args) {
	  //Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                keepRunning = false;
                if(testServerConnection != null)
                    testServerConnection.dispose();
            }
        });

        //Configure the general serializer by adding a kryo serializer
        NimrodObjectSerializer.GetInstance().getSerializers().put("kryo",new KryoSerializer());
        testServerConnection = new ZeroMQRmiClient();
        testServerConnection.setInstanceName("TestServerConnection");
        testServerConnection.setServerSocket(System.getProperty("rmiServerSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/rmiServerSocket"));
        try {
            testServerConnection.initialize();
            while(keepRunning) {
                try {
                    String response = (String)testServerConnection.executeRmiMethod(String.class, "ANDYTEST","rmiTestMethod1", "hello");
                    System.out.println("response = "+response);
                } catch (NimrodRmiNotConnectedException e) {
                    System.out.println("Server not detected .. try again");
                }
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        testServerConnection.dispose();
        System.out.println("TestClient finished");
	}

}
