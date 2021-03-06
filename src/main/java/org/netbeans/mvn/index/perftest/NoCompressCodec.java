/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.netbeans.mvn.index.perftest;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsFormat;

/**
 *
 * @author Tomas Zezula
 */
class NoCompressCodec
        extends FilterCodec
{

    public NoCompressCodec(final Codec delegate) {
        super("NoCompressCodec", delegate);
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return new Lucene40StoredFieldsFormat();
    }

}
