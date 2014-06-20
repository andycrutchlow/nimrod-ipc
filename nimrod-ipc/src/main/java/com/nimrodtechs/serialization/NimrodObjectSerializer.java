package com.nimrodtechs.serialization;

import java.util.HashMap;
import java.util.Map;

import com.nimrodtechs.exceptions.NimrodSerializationException;
import com.nimrodtechs.exceptions.NimrodSerializerNotFoundException;

public class NimrodObjectSerializer {
    private static Map<String, NimrodObjectSerializationInterface> serializers = new HashMap<String, NimrodObjectSerializationInterface>();

    public Map<String, NimrodObjectSerializationInterface> getSerializers() {
        return serializers;
    }

    public void setSerializers(Map<String, NimrodObjectSerializationInterface> serializers) {
        this.serializers = serializers;
    }

    private static NimrodObjectSerializer instance;
    
    NimrodObjectSerializer()
    {
        instance = this;
    }
    
    public static NimrodObjectSerializer GetInstance() {
        if(instance == null)
            new NimrodObjectSerializer(); 
        return instance;
    }
    
    public static byte[] serialize(String serializerId, Object o) throws NimrodSerializationException
    {
        NimrodObjectSerializationInterface s = serializers.get(serializerId);
        if(s == null)
            throw new NimrodSerializerNotFoundException();
        try {
            return s.serialize(o);
        } catch (Throwable t) {
            throw new NimrodSerializationException(t);
        }
    }
    
    public static Object deserialize(String serializerId, byte[] b, Class c) throws NimrodSerializationException
    {
        NimrodObjectSerializationInterface s = serializers.get(serializerId);
        if(s == null)
            throw new NimrodSerializerNotFoundException();
        try {
            return s.deserialize(b,c);
        } catch (Throwable t) {
            throw new NimrodSerializationException(t);
        }
    }
}
