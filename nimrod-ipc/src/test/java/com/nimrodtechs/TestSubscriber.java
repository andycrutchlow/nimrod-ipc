package com.nimrodtechs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nimrodtechs.ipc.MessageReceiverInterface;
import com.nimrodtechs.ipc.ZeroMQPubSubSubscriber;
import com.nimrodtechs.ipc.queue.QueueExecutor;
import com.nimrodtechs.serialization.NimrodObjectSerializer;
import com.nimrodtechs.serialization.kryo.KryoSerializer;

public class TestSubscriber implements MessageReceiverInterface {
    final static Logger logger = LoggerFactory.getLogger(TestSubscriber.class);
    static ZeroMQPubSubSubscriber subscriber;
    
	public static void main(String[] args) {
	  //Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if(subscriber != null)
                    subscriber.dispose();
            }
        });

        //Configure the general serializer by adding a kryo serializer
        NimrodObjectSerializer.GetInstance().getSerializers().put("kryo",new KryoSerializer());
        subscriber = new ZeroMQPubSubSubscriber();
        subscriber.setInstanceName("TestSubscriber");
        subscriber.setServerSocket(System.getProperty("rmiServerSocketUrl","ipc://"+System.getProperty("java.io.tmpdir")+"/TestPublisherSocket.pubsub"));
        try {
            subscriber.initialize();
            subscriber.subscribe("testsubject", new TestSubscriber(), String.class,QueueExecutor.CONFLATING_QUEUE);
            subscriber.subscribe("testsubject2", new TestSubscriber(), String.class);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	}

    @Override
    public void messageReceived(String subject, Object message) {
        logger.info("subject="+subject+" message="+message);
        
    }

}
