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

import com.nimrodtechs.exceptions.NimrodSerializationException;
import com.nimrodtechs.exceptions.NimrodSerializerNotFoundException;
import com.nimrodtechs.serialization.kryo.KryoSerializer;

import java.util.HashMap;
import java.util.Map;

public class NimrodObjectSerializer {

    public final static String DEFAULT_SERIALIZATION_ID = "kryo";

    private static Map<String, NimrodObjectSerializationInterface> serializers = new HashMap<>();
    private static NimrodObjectSerializer instance;

    private NimrodObjectSerializer() {
        instance = this;
        serializers.put(DEFAULT_SERIALIZATION_ID, new KryoSerializer());
    }

    public static NimrodObjectSerializer GetInstance() {
        if (instance == null)
            new NimrodObjectSerializer();
        return instance;
    }

    public static byte[] serialize(final String serializerId, final Object o) throws NimrodSerializationException {
        final NimrodObjectSerializationInterface s = serializers.get(serializerId);
        if (s == null)
            throw new NimrodSerializerNotFoundException();
        try {
            return s.serialize(o);
        }
        catch (final Throwable t) {
            throw new NimrodSerializationException(t);
        }
    }

    public static <T> T deserialize(final String serializerId, final byte[] b, final Class<T> c) throws NimrodSerializationException {
        final NimrodObjectSerializationInterface s = serializers.get(serializerId);
        if (s == null)
            throw new NimrodSerializerNotFoundException();
        try {
            return s.deserialize(b, c);
        }
        catch (final Throwable t) {
            throw new NimrodSerializationException(t);
        }
    }

    public static void register(final String serializerId, final Class c, final int id) {
        final NimrodObjectSerializationInterface s = serializers.get(serializerId);
        if (s == null)
            return;
        try {
            s.register(c, id);
        }
        catch (final Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public static void register(final String serializerId, final Class c, final Object o, final int id) {
        final NimrodObjectSerializationInterface s = serializers.get(serializerId);
        if (s == null)
            return;
        try {
            s.register(c, o, id);
        }
        catch (final Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public Map<String, NimrodObjectSerializationInterface> getSerializers() {
        return serializers;
    }

    public void setSerializers(final Map<String, NimrodObjectSerializationInterface> serializers) {
        NimrodObjectSerializer.serializers = serializers;
    }
}
