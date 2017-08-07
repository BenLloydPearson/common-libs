//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.gravity.utilities;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public class HeaderlessGZIPInputStream extends InflaterInputStream {
    protected boolean eos;
    private boolean closed;

    private void ensureOpen() throws IOException {
        if(this.closed) {
            throw new IOException("Stream closed");
        }
    }

    public HeaderlessGZIPInputStream(InputStream var1, int var2) throws IOException {
        super(var1, new Inflater(true), var2);
        this.closed = false;
    }

    public HeaderlessGZIPInputStream(InputStream var1) throws IOException {
        this(var1, 512);
    }

    public int read(byte[] var1, int var2, int var3) throws IOException {
        this.ensureOpen();
        if(this.eos) {
            return -1;
        } else {
            int var4 = super.read(var1, var2, var3);
            if(var4 == -1) {
                this.eos = true;
            }
            return var4;
        }
    }

    public void close() throws IOException {
        if(!this.closed) {
            super.close();
            this.eos = true;
            this.closed = true;
        }

    }

}
