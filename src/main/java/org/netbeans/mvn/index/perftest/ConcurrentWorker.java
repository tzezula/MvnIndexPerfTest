/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.netbeans.mvn.index.perftest;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;

/**
 *
 * @author Tomas Zezula
 */
final class ConcurrentWorker implements Worker {

    @Override
    public int index(File gz, IndexWriter index) throws IOException {
        final int throghput = Runtime.getRuntime().availableProcessors();
        final ExecutorService es = Executors.newFixedThreadPool(throghput);
        final CompletionService<Integer> cs = new ExecutorCompletionService<>(es);
        final DocumentProvider dp = new DocumentProvider(gz);
        for (int i=0; i < throghput; i++) {
            cs.submit(() -> {
                int count = 0;
                Document doc;
                while ((doc = dp.get()) != null) {
                    count++;
                    index.addDocument(doc);
                }
                return count;
            });
        }
        try {
            int count = 0;
            for (int i=0; i< throghput; i++) {
                count += cs.take().get();
            }
            es.shutdown();
            es.awaitTermination(1, TimeUnit.SECONDS);
            index.close();
            return count;
        } catch (InterruptedException | CancellationException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    private final class DocumentProvider implements Supplier<Document> {

        private final Iterator<Document> it;

        DocumentProvider(File gz) throws IOException {
            it = new IndexFileReader(gz).iterator();
        }

        @Override
        public synchronized Document get() {
            if (it.hasNext()) {
                return it.next();
            }
            return null;
        }
    }
}
