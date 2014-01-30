/*
 * Copyright (C) 2014 Jens Bertram <code@jens-bertram.net>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.unihildesheim.io;

import java.io.OutputStream;

/**
 * ByteArrayOutputStream implementation that doesn't synchronize methods and
 * doesn't copy the data on toByteArray().
 *
 * Based on http://javatechniques.com/blog/faster-deep-copies-of-java-objects/
 * and http://java-performance.info/java-io-bytearrayoutputstream/
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class FastByteArrayOutputStream extends OutputStream {

  /**
   * Byte buffer.
   */
  private byte[] buf = null;

  /**
   * Current size of the buffer.
   */
  private int size = 0;

  /**
   * Constructs a stream with buffer capacity size 5K.
   */
  public FastByteArrayOutputStream() {
    this(5 * 1024);
  }

  /**
   * Constructs a stream with the given initial size.
   *
   * @param initSize Initial size of the buffer
   */
  public FastByteArrayOutputStream(final int initSize) {
    this.size = 0;
    this.buf = new byte[initSize];
  }

  /**
   * Ensures that we have a large enough buffer for the given size.
   *
   * @param expected Size to expect
   */
  private void verifyBufferSize(final int expected) {
    if (expected > buf.length) {
      byte[] old = buf;
      buf = new byte[Math.max(expected, 2 * buf.length)];
      System.arraycopy(old, 0, buf, 0, old.length);
    }
  }

  /**
   * Returns the byte array containing the written data. Note that this array
   * will almost always be larger than the amount of data actually written.
   *
   * @return Reference to the bytes buffer
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public byte[] getByteArray() {
    return this.buf;
  }

  @Override
  public void write(final byte[] b) {
    verifyBufferSize(this.size + b.length);
    System.arraycopy(b, 0, this.buf, this.size, b.length);
    this.size += b.length;
  }

  @Override
  public void write(final byte[] b, final int off, final int len) {
    verifyBufferSize(this.size + len);
    System.arraycopy(b, off, this.buf, this.size, len);
    this.size += len;
  }

  @Override
  public void write(final int b) {
    verifyBufferSize(this.size + 1);
    this.buf[size++] = (byte) b;
  }
}
