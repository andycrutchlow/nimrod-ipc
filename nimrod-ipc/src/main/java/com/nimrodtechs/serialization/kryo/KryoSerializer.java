package com.nimrodtechs.serialization.kryo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.serialize.BigDecimalSerializer;
import com.esotericsoftware.kryo.serialize.DateSerializer;
import com.nimrodtechs.serialization.NimrodObjectSerializationInterface;


public class KryoSerializer implements NimrodObjectSerializationInterface {
    
    private Kryo kryo = new Kryo();
    
    public KryoSerializer() {
        super();
        kryo.register( BigDecimal.class, new BigDecimalSerializer() );
        kryo.register( Date.class, new DateSerializer() );
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
    }

    public byte[] serialize(Object o) {
        ObjectBuffer buffer = new ObjectBuffer(kryo, 1024, Integer.MAX_VALUE);
        byte[] bytes = buffer.writeObject(o);
        return bytes;
    }

    public Object deserialize(byte[] b, Class c) {
        ObjectBuffer buffer = new ObjectBuffer(kryo, 1024, Integer.MAX_VALUE);
        return buffer.readObject(b, c);
    }

}
