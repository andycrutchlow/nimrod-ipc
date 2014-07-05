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