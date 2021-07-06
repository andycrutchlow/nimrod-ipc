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
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


/**
 * ClassSerializer serialize java Class
 */
public class ClassSerializer extends Serializer {

    @Override
    public Object read(Kryo arg0, Input input, Class arg2) {
        try {
            return Class.forName(input.readString());
        }
        catch (Throwable t) {
            try {
                throw new RuntimeException("Unable to create Class " + input.readString(), t);
            }
            catch (Throwable t1) {
                throw new RuntimeException("Unable to create Class ", t);
            }
        }
    }

    @Override
    public void write(Kryo arg0, Output output, Object object) {
        Class value = (Class) object;
        output.writeString(value.getName());

    }
}