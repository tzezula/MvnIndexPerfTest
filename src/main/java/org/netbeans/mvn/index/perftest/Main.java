/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.netbeans.mvn.index.perftest;

import java.io.File;
import java.io.IOException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author Tomas Zezula
 */
public class Main {
    public static void main(String... args) throws IOException, InterruptedException {
        if (args.length != 2) {
            usage();
        }
        final File gz = new File(args[0]);
        if (!gz.canRead()) {
            error(
                    String.format("Cannot read file: %s", gz.getAbsolutePath()),
                    2);
        }
        final File index = mkdir(new File(args[1]));

        final Worker w = new SequentialWorker();
        long st = System.currentTimeMillis();
        int cnt = w.index(gz, createIndexWriter(index));
        long et = System.currentTimeMillis();
        System.out.printf("Indexed: %d documents in %d seconds.%n",
                cnt,
                (et-st)/1_000);
    }

    private static IndexWriter createIndexWriter(File file) throws IOException {
        final FSDirectory out = FSDirectory.open(file);
        final IndexWriterConfig cfg = new IndexWriterConfig(
                Version.LUCENE_CURRENT,
                new StandardAnalyzer(Version.LUCENE_CURRENT));
        cfg.setCodec(new NoCompressCodec(new Lucene46Codec()));
        return new IndexWriter(out, cfg);
    }

    private static File mkdir(File folder) {
        boolean needsDelete = folder.isFile() || (folder.isDirectory() && folder.listFiles().length > 0);
        boolean needsCreate = !folder.exists();
        if (needsDelete) {
            delete(folder);
            needsCreate = true;
        }
        if (needsCreate) {
            folder.mkdirs();
        }
        return folder;
    }

    private static void delete(File f) {
        if (f.isDirectory()) {
            File[] children = f.listFiles();
            if (children != null) {
                for (File cld : children) {
                    delete(cld);
                }
            }
        }
        f.delete();
    }

    private static void error(
        final String message,
        final int exitCode) {
        System.err.println(message);
        System.exit(exitCode);
    }

    private static void usage() {
        error(
                "usage: MvnIndexPerfTest mvn-repo-index.gz index-folder",
                1);
    }
}
