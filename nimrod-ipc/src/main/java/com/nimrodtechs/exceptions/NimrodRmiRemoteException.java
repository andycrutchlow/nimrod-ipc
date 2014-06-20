package com.nimrodtechs.exceptions;

public class NimrodRmiRemoteException extends NimrodRmiException {
    String remoteExceptionName;
    byte[] remoteExceptionAsBytes;
    public String getRemoteExceptionName()
    {
        return remoteExceptionName;
    }
    public byte[] getRemoteExceptionAsBytes()
    {
        return remoteExceptionAsBytes;
    }
    public NimrodRmiRemoteException() {
        super();
        // TODO Auto-generated constructor stub
    }

    public NimrodRmiRemoteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        // TODO Auto-generated constructor stub
    }

    public NimrodRmiRemoteException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

    public NimrodRmiRemoteException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    public NimrodRmiRemoteException(Throwable cause) {
        super(cause);
        // TODO Auto-generated constructor stub
    }
    
    public NimrodRmiRemoteException( String  aMessage, String remoteExceptionName, byte[] remoteExceptionAsBytes )
    {
        super( aMessage );
        this.remoteExceptionName = remoteExceptionName;
        this.remoteExceptionAsBytes = remoteExceptionAsBytes;
    }
}
