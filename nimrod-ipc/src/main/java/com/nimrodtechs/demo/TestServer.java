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

package com.nimrodtechs.demo;

import com.nimrodtechs.annotations.ExposedMethod;
import com.nimrodtechs.annotations.ExposedServiceName;
import com.nimrodtechs.ipc.ZeroMQRmiServer;
import com.nimrodtechs.serialization.NimrodObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

@ExposedServiceName(serviceName = "ANDYTEST")
public class TestServer {
    private static Logger logger = LoggerFactory.getLogger(TestServer.class);
    static ZeroMQRmiServer server;
    
    public static void main(String[] args) {
        if(args.length == 0) {
            System.out.println("Provide argument which is describes the Socket that this publisher will publish on e.g. tcp://localhost:6062");
            System.exit(0);
        }
        //Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if(server != null)
                    server.dispose();
            }
        });
        //Configure the general serializer by adding a kryo serializer
        //NimrodObjectSerializer.GetInstance().getSerializers().put("kryo",new KryoSerializer());
        //Create a RMI Server
        server = new ZeroMQRmiServer();
        //Add an instance of this TestServer class as a service and its 2 associated exposed methods
        server.addExposedService(new TestServer());
        //Set the external socket that this rmi server is accessible on..default to unix domain socket located in tmp directory
        //server.setServerSocket(System.getProperty("rmiServerSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/TestServerSocket.rmi"));
        server.setServerSocket(args[0]);
        server.setInstanceName("testserver");
        try {
            //Decide if we are using agent
            //server.setUseAgent(true);
            //Initialize
            server.initialize();
            //Indicate that we are open for requests..this might be sometime later after more internal initialization is complete
            server.setEnabled(true);
       } catch (Exception e) {
            //
            e.printStackTrace();
        }
    }
    
    /**
     * This is boiler-plate wrapper around the actual call to actual method.
     * Its job is to understand and prepare the parameters, make the actual method call and then
     * prepare the return value.
     * TODO : turn this into generated code via annotation processor with annotation just being on the actual target method.
     * All the information should be available i.e. the parameter types and values, the actual method to call, the return type etc.
     * @param params
     * @return
     * @throws Exception
     */
    @ExposedMethod
    public List<byte[]> rmiTestMethod1(List<byte[]> params) throws Exception {
        String serializerId = new String(params.get(0));
        //Get the first serialized parameter..which we know is a string
        String param1 = (String) NimrodObjectSerializer.deserialize(serializerId, params.get(1), String.class);
        //Call the actual method
        String result = rmiTestMethod1(param1);
        List<byte[]> resultArray = new ArrayList<byte[]>();
        //Prepare the result
        resultArray.add(NimrodObjectSerializer.serialize(serializerId, result));
        return resultArray;
    }
    Random r1 = new Random();
    /**
     * This is the actual remote method we are calling
     * @param param
     * @return
     */
    public String rmiTestMethod1(String param) {
        long delay = 0;
        try {
          //Simulate some work with a random delay between 1 and 20 millis
            delay = r1.nextInt(19) + 1;
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            //
            e.printStackTrace();
        }
        logger.info("rmiTestMethod1 called with param ["+param+"] simulated work took "+delay+"ms");
        return Thread.currentThread().getName()+" rmiTestMethod1 received msg ["+param+"] at "+new Date().toString()+" simulated work took "+delay+"ms";
   }
    
    @ExposedMethod
    public List<byte[]> rmiTestMethod2(List<byte[]> params) throws Exception {
        String serializerId = new String(params.get(0));
        //Get the first serialized parameter..which we know is a string
        String str1 = (String) NimrodObjectSerializer.deserialize(serializerId, params.get(1),String.class);
        //Get the second serialized parameter..which we know is a BigDecimal
        BigDecimal bd1 = (BigDecimal) NimrodObjectSerializer.deserialize(serializerId, params.get(2),BigDecimal.class);
        //Get the third serialized parameter..which we know is a String
        String str2 = (String) NimrodObjectSerializer.deserialize(serializerId, params.get(3),String.class);
        //Call the actual method
        String result = rmiTestMethod2(str1,bd1,str2);
        List<byte[]> resultArray = new ArrayList<byte[]>();
        //Prepare the result
        resultArray.add(NimrodObjectSerializer.serialize(serializerId, result));
        return resultArray;
    }
    Random r2 = new Random();
    /**
     * This is the actual remote method we are calling. 3 parameters.
     * @param str1
     * @param bd1
     * @param str2
     * @return
     */
    public String rmiTestMethod2(String str1, BigDecimal bd1, String str2 ){
        //Simulate some work with a random delay between 1 and 20 millis
        long delay = 0;
        try {
          //Simulate some work with a random delay between 1 and 20 millis
            delay = r2.nextInt(19) + 1;
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            //
            e.printStackTrace();
        }
        logger.info("rmiTestMethod2 called ["+str1+"] simulated work took "+delay+"ms");
        return Thread.currentThread().getName()+" rmiTestMethod2 received msg ["+str1+"]"+"["+bd1+"]"+"["+str2+"] at "+new Date().toString()+" simulated work took "+delay+"ms";
    }
    
}
