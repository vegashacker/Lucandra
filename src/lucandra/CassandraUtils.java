/**
 * Copyright 2009 T Jake Luciani
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package lucandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.service.StorageService;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;

public class CassandraUtils {

    public static final String keySpace            = "Lucandra";
    public static final String termVecColumnFamily = "TermVectors";
    public static final String docColumnFamily     = "Documents";
    public static final String delimeter           = ""+new Character((char)255)+new Character((char)255);
    public static final String documentIdField     = "__KEY__";
    

    private static final Logger logger = Logger.getLogger(CassandraUtils.class);

    //Start on startup
    static{
        try{
           StorageService.instance().start();
        }catch(IOException e){
            throw new RuntimeException("Unable to start cassandra",e);
        }
    }

    public static String createColumnName(Term term) {
        return createColumnName(term.field(), term.text());
    }

    public static String createColumnName(String field, String text) {
        return field + delimeter + text;
    }

    public static Term parseTerm(byte[] termStr) {
        String[] parts = null;

       
        parts = new String(termStr).split(delimeter);
        

        if (parts == null || parts.length != 2) {
            throw new RuntimeException("invalid term format: " + termStr);
        }

        return new Term(parts[0].intern(), parts[1]);
    }

    public static final byte[] intToByteArray(int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    }

    public static final int byteArrayToInt(byte[] b) {
        return (b[0] << 24) + ((b[1] & 0xFF) << 16) + ((b[2] & 0xFF) << 8) + (b[3] & 0xFF);
    }

    public static final byte[] intVectorToByteArray(List<Integer> intVector) {
        ByteBuffer buffer = ByteBuffer.allocate(4 * intVector.size());

        for (int i : intVector) {
            buffer.putInt(i);
        }

        return buffer.array();
    }

    public static boolean compareByteArrays(byte[] a, byte[] b) {

        if (a.length != b.length)
            return false;

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i])
                return false;
        }

        return true;

    }

    public static final int[] byteArrayToIntArray(byte[] b) {

        if (b.length % 4 != 0)
            throw new RuntimeException("Not a valid int array:" + b.length);

        int[] intArray = new int[b.length / 4];
        int idx = 0;

        for (int i = 0; i < b.length; i += 4) {
            intArray[idx] = (b[i] << 24) + ((b[i + 1] & 0xFF) << 16) + ((b[i + 2] & 0xFF) << 8) + (b[i + 3] & 0xFF);
        }

        return intArray;
    }

    public static final byte[] encodeLong(long l) {
        ByteBuffer buffer = ByteBuffer.allocate(8);

        buffer.putLong(l);

        return buffer.array();
    }

    public static final long decodeLong(byte[] bytes) {

        if (bytes.length != 8)
            throw new RuntimeException("must be 8 bytes");

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        return buffer.getLong();
    }

    public static final byte[] encodeUUID(UUID docUUID) {

        ByteBuffer buffer = ByteBuffer.allocate(16);

        buffer.putLong(docUUID.getMostSignificantBits());
        buffer.putLong(docUUID.getLeastSignificantBits());
        return buffer.array();
    }

    public static final UUID readUUID(byte[] bytes) {

        if (bytes.length != 16)
            throw new RuntimeException("uuid must be exactly 16 bytes");

        return UUID.nameUUIDFromBytes(bytes);

    }

}
