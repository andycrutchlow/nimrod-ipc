package com.nimrodtechs.exceptions;

import com.nimrodtechs.ipc.ZeroMQRmiClient;



public interface NimrodRmiEventListener {
    public void onBreakInConnection(ZeroMQRmiClient transport);
    public void onConnectionEstablished(ZeroMQRmiClient transport);

}
