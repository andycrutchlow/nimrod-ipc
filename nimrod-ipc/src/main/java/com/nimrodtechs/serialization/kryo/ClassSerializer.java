package com.nimrodtechs.serialization.kryo;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serialize.StringSerializer;

/**
 * ClassSerializer serialize java Class
 */
public class ClassSerializer extends Serializer {
    public Class readObjectData(ByteBuffer aBuffer, Class aType) {
        try {
            return Class.forName(StringSerializer.get(aBuffer));
        } catch (Throwable t) {
            throw new RuntimeException("Unable to read Class");
        }
    }

    public void writeObjectData(ByteBuffer aBuffer, Object anObject) {
        Class value = (Class) anObject;
        StringSerializer.put(aBuffer, value.getName());
    }
}