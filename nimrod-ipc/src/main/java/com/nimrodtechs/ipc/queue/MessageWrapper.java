package com.nimrodtechs.ipc.queue;

public class MessageWrapper {
	public MessageWrapper(String actualSubject, byte[] rawMessage) {
		super();
		this.actualSubject = actualSubject;
		this.rawMessage = rawMessage;
	}
	String actualSubject;
	byte[] rawMessage;
}
