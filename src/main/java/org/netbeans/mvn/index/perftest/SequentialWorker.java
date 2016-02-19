/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.netbeans.mvn.index.perftest;

import java.io.File;
import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;

/**
 *
 * @author Tomas Zezula
 */
final class SequentialWorker implements Worker {
    @Override
    public int index(File gz, IndexWriter index) throws IOException {
        int cnt = 0;
        try (IndexWriter writer = index) {
            for (Document doc : new IndexFileReader(gz)) {
                writer.addDocument(doc);
                cnt++;
            }
        }
        return cnt;
    }
}
