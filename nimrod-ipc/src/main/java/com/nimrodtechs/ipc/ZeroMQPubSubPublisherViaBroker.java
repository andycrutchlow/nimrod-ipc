package com.nimrodtechs.ipc;

public class ZeroMQPubSubPublisherViaBroker extends ZeroMQPubSubPublisher {
	public boolean initialize() throws Exception {
		setManyToOne(true);
		return super.initialize();
	}

	public void publish(String subject, Object message) {
		super.publish(ZeroMQCommon.BROKER_SUBJECT_PREFIX + subject, message);
	}
}
