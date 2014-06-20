package com.nimrodtechs.serialization;

import com.nimrodtechs.exceptions.NimrodSerializationException;

public interface NimrodObjectSerializationInterface {
    public byte[] serialize(Object o);
    
    public Object deserialize(byte[] b, Class c) throws NimrodSerializationException;
}
