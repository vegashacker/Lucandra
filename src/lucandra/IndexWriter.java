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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

public class IndexWriter {

    private final String indexName;

    private static final Logger logger = Logger.getLogger(IndexWriter.class);

    public IndexWriter(String indexName) {

        this.indexName = indexName;

    }

    public void addDocument(Document doc, Analyzer analyzer) throws CorruptIndexException, IOException {

        Token token = new Token();

        // check for special field name
        String docId = doc.get(CassandraUtils.documentIdField);

        if (docId == null)
            docId = Long.toHexString(System.nanoTime());

        QueryPath termVecColumnPath = new QueryPath(CassandraUtils.termVecColumnFamily, null, docId.getBytes());
        byte[] nullValue = CassandraUtils.intVectorToByteArray(Arrays.asList(new Integer[] { 0 }));

        int position = 0;

        for (Field field : (List<Field>) doc.getFields()) {

            // Indexed field
            if (field.isIndexed() && field.isTokenized()) {

                TokenStream tokens = field.tokenStreamValue();

                if (tokens == null) {
                    tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
                }

                // collect term frequencies per doc
                Map<String, List<Integer>> termPositions = new HashMap<String, List<Integer>>();
                int lastOffset = 0;
                if (position > 0) {
                    position += analyzer.getPositionIncrementGap(field.name());
                }

                // Build the termPositions vector for all terms
                while (tokens.next(token) != null) {
                    String term = CassandraUtils.createColumnName(field.name(), token.term());

                    List<Integer> pvec = termPositions.get(term);

                    if (pvec == null) {
                        pvec = new ArrayList<Integer>();
                        termPositions.put(term, pvec);
                    }

                    position += (token.getPositionIncrement() - 1);
                    pvec.add(++position);

                }

                for (Map.Entry<String, List<Integer>> term : termPositions.entrySet()) {

                    // Terms are stored within a unique key combination
                    // This is required since cassandra loads all column
                    // families for a key into memory
                    String key = indexName + "/" + term.getKey();

                    RowMutation rm = new RowMutation(CassandraUtils.keySpace, key);

                    rm.add(termVecColumnPath, CassandraUtils.intVectorToByteArray(term.getValue()), System.nanoTime());

                    sendRowMution(rm);
                }
            }

            // Untokenized fields go in without a termPosition
            if (field.isIndexed() && !field.isTokenized()) {
                String term = CassandraUtils.createColumnName(field.name(), field.stringValue());

                String key = indexName + "/" + term;

                RowMutation rm = new RowMutation(CassandraUtils.keySpace, key);

                rm.add(termVecColumnPath, nullValue, System.nanoTime());
                
                sendRowMution(rm);
            }            

            // Stores each field as a column under this doc key
            if (field.isStored()) {

                byte[] value = field.isBinary() ? field.getBinaryValue() : field.stringValue().getBytes();

                RowMutation rm = new RowMutation(CassandraUtils.keySpace, indexName + "/" + docId);

                rm.add(new QueryPath(CassandraUtils.docColumnFamily, null, field.name().getBytes()), value, System.nanoTime());
           
                sendRowMution(rm);
            }   
        } 
    }
    
    private void sendRowMution(RowMutation rm){
        boolean suceeded = false;
        
        while(!suceeded){
            try{
                StorageProxy.insertBlocking(rm, 1); //Send
                suceeded = true;
            }catch(UnavailableException e){
                System.err.println("write failed");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
        }       
    }
    
    public void deleteDocuments(Query query) throws CorruptIndexException, IOException {
        throw new UnsupportedOperationException();
    }

    public void deleteDocuments(Term arg0) throws CorruptIndexException, IOException {
        throw new UnsupportedOperationException();
    }

    public int docCount() {
        throw new UnsupportedOperationException();
    }

}
