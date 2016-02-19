/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.netbeans.mvn.index.perftest;

import java.io.File;
import java.io.IOException;
import org.apache.lucene.index.IndexWriter;

/**
 *
 * @author Tomas Zezula
 */
@FunctionalInterface
interface Worker {
    public int index (
            File gz,
            IndexWriter index) throws IOException ;
}
