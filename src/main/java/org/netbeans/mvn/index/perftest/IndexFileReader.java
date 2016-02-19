/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.netbeans.mvn.index.perftest;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

/**
 *
 * @author Tomas Zezula
 */
final class IndexFileReader implements Iterable<Document> {
    private static final int VERSION = 1;
    private static final int F_INDEXED = 1;
    private static final int F_TOKENIZED = 2;
    private static final int F_STORED = 4;

    private final File gz;

    IndexFileReader(File gz) {
        gz.getClass();
        this.gz = gz;
    }

    @Override
    public Iterator<Document> iterator() {
        try {
            return new Reader(gz);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private static final class Reader implements Iterator<Document> {

        DataInputStream in;
        Document nextDoc;

        Reader(final File gz) throws IOException {
            in = new DataInputStream(
                    new BufferedInputStream(new GZIPInputStream(
                            new BufferedInputStream(
                                    new FileInputStream(gz),
                                    1 << 17)),1<<17));
            readHeader(in);
        }

        @Override
        public boolean hasNext() {
            if (in == null) {
                return false;
            }
            try {
                try {
                    final Document doc = new Document();
                    int fldCnt = in.readInt();
                    for (int i = 0; i < fldCnt; i++) {
                        doc.add(readField(in));
                    }
                    nextDoc = doc;
                    return true;
                } catch (EOFException eof) {
                    try {
                        in.close();
                    } finally {
                        in = null;
                        return false;
                    }
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        @Override
        public Document next() {
            if (hasNext()) {
                Document res = nextDoc;
                nextDoc = null;
                return res;
            } else {
                throw new NoSuchElementException();
            }
        }

        private long readHeader(DataInputStream in) throws IOException {
            final byte HDRBYTE = (byte) ((VERSION << 24)>>24);
            if (HDRBYTE != in.readByte()) {
                throw new IOException( "Provided input contains unexpected data (0x01 expected as 1st byte)!" );
            }
            return in.readLong();
        }

        private static Field readField(DataInputStream in) throws IOException {
            final int flags = in.read();
            Index index = Index.NO;
            Store store = Store.NO;
            if ((flags & F_INDEXED) != 0) {
                final boolean isTokenized = (flags & F_TOKENIZED) != 0;
                index = isTokenized ? Index.ANALYZED : Index.NOT_ANALYZED;
            }
            if ((flags & F_STORED) != 0) {
                store = Store.YES;
            }
            final String name = in.readUTF();
            final String value = readUTF(in);
            return new Field(name, value, store, index);
        }

        private static String readUTF(DataInputStream in) throws IOException {
            int utflen = in.readInt();

            byte[] bytearr;
            char[] chararr;

            try {
                bytearr = new byte[utflen];
                chararr = new char[utflen];
            } catch (OutOfMemoryError e) {
                final IOException ex
                        = new IOException(
                                "Index data content is inappropriate (is junk?), leads to OutOfMemoryError! See MINDEXER-28 for more information!");
                ex.initCause(e);
                throw ex;
            }

            int c, char2, char3;
            int count = 0;
            int chararr_count = 0;

            in.readFully(bytearr, 0, utflen);

            while (count < utflen) {
                c = bytearr[count] & 0xff;
                if (c > 127) {
                    break;
                }
                count++;
                chararr[chararr_count++] = (char) c;
            }

            while (count < utflen) {
                c = bytearr[count] & 0xff;
                switch (c >> 4) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        /* 0xxxxxxx */
                        count++;
                        chararr[chararr_count++] = (char) c;
                        break;

                    case 12:
                    case 13:
                        /* 110x xxxx 10xx xxxx */
                        count += 2;
                        if (count > utflen) {
                            throw new UTFDataFormatException("malformed input: partial character at end");
                        }
                        char2 = bytearr[count - 1];
                        if ((char2 & 0xC0) != 0x80) {
                            throw new UTFDataFormatException("malformed input around byte " + count);
                        }
                        chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                        break;

                    case 14:
                        /* 1110 xxxx 10xx xxxx 10xx xxxx */
                        count += 3;
                        if (count > utflen) {
                            throw new UTFDataFormatException("malformed input: partial character at end");
                        }
                        char2 = bytearr[count - 2];
                        char3 = bytearr[count - 1];
                        if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                            throw new UTFDataFormatException("malformed input around byte " + (count - 1));
                        }
                        chararr[chararr_count++]
                                = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
                        break;

                    default:
                        /* 10xx xxxx, 1111 xxxx */
                        throw new UTFDataFormatException("malformed input around byte " + count);
                }
            }
            // The number of chars produced may be less than utflen
            return new String(chararr, 0, chararr_count);
        }
    }
}
