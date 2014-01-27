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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * ByteArrayInputStream implementation that does not synchronize methods.
 *
 * Based on http://javatechniques.com/blog/faster-deep-copies-of-java-objects/
 * and http://java-performance.info/java-io-bytearrayoutputstream/
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class FastByteArrayInputStream extends InputStream {

  /**
   * Referenced byte buffer.
   */
  private final byte[] buf;

  /**
   * Number of bytes readable from the buffer.
   */
  private int count = 0;

  /**
   * Number of bytes read from the buffer.
   */
  private int pos = 0;

  /**
   * Creates a new {@link InputStream} from the given <tt>byte</tt> array. The
   * array will only be referenced, so if it changes while reading the behavior
   * is undefined.
   *
   * @param bufToRef Buffer to read from
   */
  public FastByteArrayInputStream(final byte[] bufToRef) {
    this.buf = bufToRef;
    this.count = bufToRef.length;
  }

  @Override
  public int available() {
    return this.count - this.pos;
  }

  @Override
  public int read() {
    return (this.pos < this.count) ? (this.buf[this.pos++] & 0xff) : -1;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) {
    int lenRead = len; // amount of bytes actually read

    if (this.pos >= this.count) {
      return -1;
    }

    if ((this.pos + lenRead) > this.count) {
      lenRead = (count - pos);
    }

    System.arraycopy(buf, pos, b, off, lenRead);
    pos += lenRead;
    return lenRead;
  }

  @Override
  public long skip(final long skipBytes) {
    long numSkipped = skipBytes; // amount of bytes actually skipped

    if ((this.pos + numSkipped) > this.count) {
      numSkipped = this.count - this.pos;
    }
    if (numSkipped < 0) {
      return 0;
    }
    pos += numSkipped;
    return numSkipped;
  }
}
