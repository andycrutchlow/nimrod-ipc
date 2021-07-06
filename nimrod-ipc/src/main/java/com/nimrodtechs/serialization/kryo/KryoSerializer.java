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

package com.nimrodtechs.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.nimrodtechs.serialization.NimrodObjectSerializationInterface;
import org.hibernate.collection.internal.*;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.Consumer;

public class KryoSerializer implements NimrodObjectSerializationInterface {

    private static final List<Consumer<Kryo>> KRYO_PLUGIN_HOOKS = new ArrayList<>();

    private final ThreadLocal<KryoInfo> kryoInfos = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.register(BigDecimal.class);
        kryo.register(Date.class);
        kryo.register(Class.class, new ClassSerializer());
        kryo.register(HashMap.class);
        kryo.register(HashSet.class);
        kryo.register(Boolean[].class);
        kryo.register(Double[].class);
        kryo.register(Float[].class);
        kryo.register(Integer[].class);
        kryo.register(Long[].class);
        kryo.register(Short[].class);
        kryo.register(String[].class);
        kryo.register(Date[].class);
        kryo.register(BigDecimal[].class);
        kryo.register(BigInteger[].class);
        kryo.register(Class[].class);
        kryo.register(Object[].class);
        kryo.register(ArrayList.class);
        kryo.register(TreeMap.class);
        kryo.register(boolean[].class);
        kryo.register(double[].class);
        kryo.register(float[].class);
        kryo.register(int[].class);
        kryo.register(long[].class);
        kryo.register(short[].class);
        kryo.register(byte[].class);
        kryo.register(TreeSet.class);
        KRYO_PLUGIN_HOOKS.forEach(kryoConsumer -> kryoConsumer.accept(kryo));
        //Optionally add hibernate collection classes if hibernate on classpath
        try {
            kryo.addDefaultSerializer(PersistentIdentifierBag.class, new FieldSerializer(kryo, PersistentIdentifierBag.class));
            kryo.addDefaultSerializer(PersistentBag.class, new FieldSerializer(kryo, PersistentBag.class));
            kryo.addDefaultSerializer(PersistentList.class, new FieldSerializer(kryo, PersistentList.class));
            kryo.addDefaultSerializer(PersistentSet.class, new FieldSerializer(kryo, PersistentSet.class));
            kryo.addDefaultSerializer(PersistentMap.class, new FieldSerializer(kryo, PersistentMap.class));
            kryo.addDefaultSerializer(PersistentSortedMap.class, new FieldSerializer(kryo, PersistentSortedMap.class));
            kryo.addDefaultSerializer(PersistentSortedSet.class, new FieldSerializer(kryo, PersistentSortedSet.class));
        } catch (NoClassDefFoundError e) {
            //Don't care ...just means hibernate entities containing collection classes can't be deserialized

        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        return new KryoInfo(
                kryo,
                outputStream,
                new UnsafeOutput(outputStream),
                new UnsafeInput()
        );
    });

    private static class KryoInfo {

        private final Kryo kryo;
        private final ByteArrayOutputStream outputStream;
        private final Output output;
        private final Input input;

        KryoInfo(
                final Kryo kryo,
                final ByteArrayOutputStream outputStream,
                final Output output,
                final Input input
        ) {
            this.kryo = kryo;
            this.outputStream = outputStream;
            this.output = output;
            this.input = input;
        }
    }

    public KryoSerializer() {
    }

    public static void addKryoPluginHook(final Consumer<Kryo> pluginHook) {
        KRYO_PLUGIN_HOOKS.add(pluginHook);
    }

    public byte[] serialize(final Object object) {
        if (object instanceof String) {
            return ((String) object).getBytes();
        }
        else {
            final KryoInfo kryoInfo = kryoInfos.get();
            final Kryo kryo = kryoInfo.kryo;
            final Output output = kryoInfo.output;
            final ByteArrayOutputStream outputStream = kryoInfo.outputStream;
            output.clear();
            outputStream.reset();

            if (object == null) {
                kryo.writeObject(output, Kryo.NULL);
            }
            else {
                kryo.writeObjectOrNull(output, object, object.getClass());
            }
            output.flush();
            return outputStream.toByteArray();
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T deserialize(final byte[] inputBytes, final Class<T> tClass) {
        if (String.class.equals(tClass)) {
            return (T) new String(inputBytes);
        }
        else {
            final KryoInfo kryoInfo = kryoInfos.get();
            final Kryo kryo = kryoInfo.kryo;
            final Input input = kryoInfo.input;
            input.setBuffer(inputBytes);
            return kryo.readObjectOrNull(input, tClass);
        }
    }

    @Override
    public void register(final Class c, final int id) {
    }

    @Override
    public void register(final Class c, final Object serializer, final int id) {
    }
}
