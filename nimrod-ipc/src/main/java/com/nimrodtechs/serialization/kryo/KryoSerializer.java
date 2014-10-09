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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeMemoryInput;
import com.esotericsoftware.kryo.io.UnsafeMemoryOutput;
import com.nimrodtechs.serialization.NimrodObjectSerializationInterface;


public class KryoSerializer implements NimrodObjectSerializationInterface {
    
    //private Kryo kryo = new Kryo();
    
    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            //kryo.setRegistrationRequired(true);
            kryo.register( BigDecimal.class );
            kryo.register( Date.class );
            kryo.register( Class.class, new ClassSerializer() );
            kryo.register( HashMap.class );
            kryo.register( HashSet.class );
            kryo.register( Boolean[].class );
            kryo.register( Double[].class );
            kryo.register( Float[].class );
            kryo.register( Integer[].class );
            kryo.register( Long[].class );
            kryo.register( Short[].class );
            kryo.register( String[].class );
            kryo.register( Date[].class );
            kryo.register( BigDecimal[].class );
            kryo.register( BigInteger[].class );
            kryo.register( Class[].class );
            kryo.register( Object[].class );
            kryo.register( ArrayList.class );
            kryo.register( TreeMap.class );
            kryo.register( boolean[].class );
            kryo.register( double[].class );
            kryo.register( float[].class );
            kryo.register( int[].class );
            kryo.register( long[].class );
            kryo.register( short[].class );
            kryo.register( byte[].class );
            kryo.register( TreeSet.class );

            //register extra stuff ..
            
            
            
            return kryo;
        };
    };
    
    public KryoSerializer() {
        super();
//        kryo.setRegistrationRequired(true);
//        kryo.register( BigDecimal.class );
//        kryo.register( Date.class );
//        kryo.register( Class.class, new ClassSerializer() );
//        kryo.register( HashMap.class );
//        kryo.register( HashSet.class );
//        kryo.register( Boolean[].class );
//        kryo.register( Double[].class );
//        kryo.register( Float[].class );
//        kryo.register( Integer[].class );
//        kryo.register( Long[].class );
//        kryo.register( Short[].class );
//        kryo.register( String[].class );
//        kryo.register( Date[].class );
//        kryo.register( BigDecimal[].class );
//        kryo.register( BigInteger[].class );
//        kryo.register( Class[].class );
//        kryo.register( Object[].class );
//        kryo.register( ArrayList.class );
//        kryo.register( TreeMap.class );
//        kryo.register( boolean[].class );
//        kryo.register( double[].class );
//        kryo.register( float[].class );
//        kryo.register( int[].class );
//        kryo.register( long[].class );
//        kryo.register( short[].class );
//        kryo.register( byte[].class );
//        kryo.register( TreeSet.class );
    }

    public byte[] serialize(Object o) {
    	
    	Kryo kryo = kryos.get();
    	
    	if(o instanceof String) {
    		return ((String)o).getBytes();
    	} else {
    		ByteArrayOutputStream stream = new ByteArrayOutputStream();
    		//Output output = new Output(stream);
    		UnsafeMemoryOutput output = new UnsafeMemoryOutput(stream);
    		kryo.writeObject(output, o);
    		output.close();
    		return stream.toByteArray();
    	}
    }

    public Object deserialize(byte[] b, Class c) {
    	Kryo kryo = kryos.get();
    	if(c.isInstance("") ) {
    		return new String(b);
    	} else {
    		ByteArrayInputStream stream = new ByteArrayInputStream(b);
    		//Input input = new Input(stream);
    		UnsafeMemoryInput input = new UnsafeMemoryInput(stream);
    		Object result = kryo.readObject(input, c);
    		input.close();
    		return result;
    	}
     }
    public void register(Class c, int id) {
//    	kryo.register(c, id);
    }
    public void register(Class c, Object object, int id) {
//    	Serializer serializer = (Serializer)object;
 //   	kryo.register(c, serializer, id);
    }
}
