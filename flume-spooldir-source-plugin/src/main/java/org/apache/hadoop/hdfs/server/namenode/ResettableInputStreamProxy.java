package org.apache.hadoop.hdfs.server.namenode;

import org.apache.flume.serialization.ResettableInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author wang_zh
 * @date 2020/5/26
 */
public class ResettableInputStreamProxy<T extends ResettableInputStream> extends InputStream {

    private T in;

    public ResettableInputStreamProxy(T is) {
        this.in = is;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
    }

//    @Override
//    public int available() throws IOException {
//        return in.available();
//    }

    @Override
    public int available() throws IOException {
        return Integer.MAX_VALUE;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        try {
            in.mark();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void mark() throws IOException {
        in.mark();
    }


    @Override
    public synchronized void reset() throws IOException {
        in.reset();
    }

    @Override
    public boolean markSupported()  {
        return true;
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }
}
