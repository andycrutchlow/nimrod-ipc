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
