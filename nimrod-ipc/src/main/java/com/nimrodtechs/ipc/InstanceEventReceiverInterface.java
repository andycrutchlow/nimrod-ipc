package com.nimrodtechs.ipc;

public interface InstanceEventReceiverInterface {
    public static int INSTANCE_UP = 0;
    public static int INSTANCE_DOWN = 1;
    void instanceEvent(String instanceName, String instanceUrl, int instanceStatus);
}
