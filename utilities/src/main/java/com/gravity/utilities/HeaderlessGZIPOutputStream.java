//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.gravity.utilities;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

public class HeaderlessGZIPOutputStream extends DeflaterOutputStream {
    protected CRC32 crc;

    public HeaderlessGZIPOutputStream(OutputStream var1, int var2) throws IOException {
        super(var1, new Deflater(-1, true), var2);
    }

    public HeaderlessGZIPOutputStream(OutputStream var1) throws IOException {
        this(var1, 512);
    }

    public synchronized void write(byte[] var1, int var2, int var3) throws IOException {
        super.write(var1, var2, var3);
    }

    public void finish() throws IOException {
        if(!this.def.finished()) {
            this.def.finish();

            while(!this.def.finished()) {
                int var1 = this.def.deflate(this.buf, 0, this.buf.length);
                if(this.def.finished() && var1 <= this.buf.length - 8) {
                    this.out.write(this.buf, 0, var1);
                    return;
                }

                if(var1 > 0) {
                    this.out.write(this.buf, 0, var1);
                }
            }
        }

    }

}
