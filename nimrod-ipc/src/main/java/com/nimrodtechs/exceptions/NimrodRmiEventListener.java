package com.nimrodtechs.exceptions;

import com.nimrodtechs.rmi.zmq.ZeroMQRmiClient;



public interface NimrodRmiEventListener {
    public void onBreakInConnection(ZeroMQRmiClient transport);
    public void onConnectionEstablished(ZeroMQRmiClient transport);

}
