package com.nimrodtechs;

import com.nimrodtechs.ipc.ZeroMQPubSubPublisher;
import com.nimrodtechs.serialization.NimrodObjectSerializer;
import com.nimrodtechs.serialization.kryo.KryoSerializer;

public class TestPublisher {
    static ZeroMQPubSubPublisher publisher;
    public static void main(String[] args) {
        //Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if(publisher != null)
                    publisher.dispose();
            }
        });
        //Configure the general serializer by adding a kryo serializer
        NimrodObjectSerializer.GetInstance().getSerializers().put("kryo",new KryoSerializer());
        
        publisher = new ZeroMQPubSubPublisher();
        publisher.setServerSocket(System.getProperty("rmiServerSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/TestPublisherSocket.pubsub"));
        publisher.setInstanceName("testpublisher");
        try {
            //Initialize
            publisher.initialize();
            for(int i=0;i<5;i++){
                publisher.publish("testsubject", "testmessage");
                publisher.publish("testsubject2", "testmessage2");
            }
            publisher.dispose();
            
       } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }



    }

}
