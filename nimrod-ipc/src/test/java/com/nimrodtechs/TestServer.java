package com.nimrodtechs;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.nimrodtechs.annotations.ExposedMethod;
import com.nimrodtechs.annotations.ExposedServiceName;
import com.nimrodtechs.rmi.zmq.ZeroMQRmiServer;
import com.nimrodtechs.serialization.NimrodObjectSerializer;
import com.nimrodtechs.serialization.kryo.KryoSerializer;

@ExposedServiceName(serviceName = "ANDYTEST")
public class TestServer {
    static ZeroMQRmiServer server;

    public static void main(String[] args) {
        //Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if(server != null)
                    server.dispose();
            }
        });
        //Configure the general serializer by adding a kryo serializer
        NimrodObjectSerializer.GetInstance().getSerializers().put("kryo",new KryoSerializer());
        //Create a RMI Server
        server = new ZeroMQRmiServer();
        //Add an instance of this TestServer class as a service and its 2 associated exposed methods
        server.addExposedService(new TestServer());
        //Set the external socket that this rmi server is accessible on..default to unix domain socket located in tmp directory
        server.setServerRmiSocket(System.getProperty("rmiServerSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/rmiServerSocket"));
        try {
            //Initialize
            server.initialize();
            //Indicate that we are open for requests..this might be sometime later after more internal initialization is complete
            server.setEnabled(true);
       } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    /**
     * This is boiler-plate wrapper around the actual call to actual method.
     * Its job is to understand and prepare the parameters, make the actual method call and then
     * prepare the return value.
     * Want to turn this into generated code via annotation processor with annotation just being on the actual target method.
     * All the information should be available i.e. the parameter types and values, the actual method to call, the return type etc.
     * @param params
     * @return
     * @throws Exception
     */
    @ExposedMethod
    public List<byte[]> rmiTestMethod1(List<byte[]> params) throws Exception {
        String serializerId = new String(params.get(0));
        //Get the first serialized parameter..which we know is a string
        String param1 = (String)NimrodObjectSerializer.deserialize(serializerId, params.get(1), String.class);
        //Call the actual method
        String result = rmiTestMethod1(param1);
        List<byte[]> resultArray = new ArrayList<byte[]>();
        //Prepare the result
        resultArray.add(NimrodObjectSerializer.serialize(serializerId, result));
        return resultArray;
    }
    /**
     * This is the actual remote method we are calling
     * @param param
     * @return
     */
    public String rmiTestMethod1(String param){
        return System.currentTimeMillis()+" received msg ["+param+"]";
    }
    
    @ExposedMethod
    public List<byte[]> rmiTestMethod2(List<byte[]> params) throws Exception {
        String serializerId = new String(params.get(0));
        //Get the first serialized parameter..which we know is a string
        String str1 = (String)NimrodObjectSerializer.deserialize(serializerId, params.get(1),String.class);
        //Get the second serialized parameter..which we know is a BigDecimal
        BigDecimal bd1 = (BigDecimal)NimrodObjectSerializer.deserialize(serializerId, params.get(2),BigDecimal.class);  
        //Get the third serialized parameter..which we know is a String
        String str2 = (String)NimrodObjectSerializer.deserialize(serializerId, params.get(3),String.class);
        //Call the actual method
        String result = rmiTestMethod2(str1,bd1,str2);
        List<byte[]> resultArray = new ArrayList<byte[]>();
        //Prepare the result
        resultArray.add(NimrodObjectSerializer.serialize(serializerId, result));
        return resultArray;
    }
    /**
     * This is the actual remote method we are calling. 3 parameters.
     * @param str1
     * @param bd1
     * @param str2
     * @return
     */
    public String rmiTestMethod2(String str1, BigDecimal bd1, String str2 ){
        return System.currentTimeMillis()+" received msg ["+str1+"]"+"["+bd1+"]"+"["+str2+"]";
    }
    
}
