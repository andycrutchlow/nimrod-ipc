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

package com.nimrodtechs.serialization;

import java.util.HashMap;
import java.util.Map;

import com.nimrodtechs.exceptions.NimrodSerializationException;
import com.nimrodtechs.exceptions.NimrodSerializerNotFoundException;
import com.nimrodtechs.serialization.kryo.KryoSerializer;

public class NimrodObjectSerializer {
	
    public final static String DEFAULT_SERIALIZATION_ID = "kryo";

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
        serializers.put(DEFAULT_SERIALIZATION_ID,new KryoSerializer());
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

    public static void register(String serializerId, Class c, int id)
    {
        NimrodObjectSerializationInterface s = serializers.get(serializerId);
        if(s == null)
            return;
        try {
            s.register(c,id);
        } catch (Throwable t) {
            return;
        }
    }
    
    public static void register(String serializerId, Class c, Object o, int id)
    {
        NimrodObjectSerializationInterface s = serializers.get(serializerId);
        if(s == null)
            return;
        try {
            s.register(c,o,id);
        } catch (Throwable t) {
            return;
        }
    }

}
