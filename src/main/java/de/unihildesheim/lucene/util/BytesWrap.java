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
package de.unihildesheim.lucene.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static wrapper for Lucenes {@link BytesRef}. Based on ideas from:
 * https://stackoverflow.com/a/1058169.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class BytesWrap implements Serializable, Comparable<BytesWrap>,
        Cloneable {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          BytesWrap.class);

//  private static final int UNSIGNED_MASK = 0xFF;

  private static transient final Map<byte[], String> intern
          = Collections.synchronizedMap(new InternMap<byte[], String>(100000));

  /**
   * Serialization class version id.
   */
  private static final long serialVersionUID = 0L;

  /**
   * Copy of the original byte array passed at construction time.
   */
  private final byte[] data;

  /**
   * Pre-calculated hash code of this instance. A hash code of 0 means that
   * the bytes of this instance are referenced and a hash code must be
   * calculated ad-hoc.
   */
  private transient Integer hash;

  /**
   * Creates a new wrapper around the given bytes array, making a local copy
   * of the array.
   *
   * @param existingBytes Byte array to wrap
   */
  public BytesWrap(final byte[] existingBytes) {
    if (existingBytes == null || existingBytes.length == 0) {
      throw new IllegalArgumentException("Empty bytes given.");
    }

    this.data = snapshot(existingBytes);
    if (this.data == null || this.data.length == 0) {
      throw new IllegalArgumentException("Got zero bytes.");
    }
    this.hash = Arrays.hashCode(this.data);
  }

  /**
   * Creates a wrapper around the byte array contained in the given
   * {@link BytesRef}, creating a local copy of the array. This is only a
   * snapshot, taking the offset and length of the byte array as defined when
   * calling this constructor. Those values won't be updated anymore.
   *
   * @param bytesRef BytesRef instance with bytes to wrap
   */
  public BytesWrap(final BytesRef bytesRef) {
    if (bytesRef == null || bytesRef.length == 0
            || bytesRef.bytes.length == 0) {
      throw new IllegalArgumentException("Empty bytes given.");
    }

    this.data = snapshot(bytesRef.bytes, bytesRef.offset, bytesRef.length
            - bytesRef.offset);
    if (this.data == null || this.data.length == 0) {
      throw new IllegalArgumentException("Got zero bytes.");
    }
    this.hash = Arrays.hashCode(this.data);
  }

  /**
   * Create a local copy of (a portion of) a given array.
   *
   * @param toClone Array to clone
   * @param offset Offset to start copy from
   * @param length Number of bytes to copy
   * @return Copy of the input array as defined
   */
  private byte[] snapshot(final byte[] toClone, final int offset,
          final int length) {
    final byte[] newData = new byte[length];
    System.arraycopy(toClone, offset, newData, 0, length);
    return newData;
  }

  /**
   * Creates a full copy of the given array.
   *
   * @param toClone Array to copy
   * @return Copy of the given array
   */
  private byte[] snapshot(final byte[] toClone) {
    return snapshot(toClone, 0, toClone.length);
  }

  /**
   * Create a cloned copy of this instance.
   *
   * @return Instance with an independent byte array cloned from the current
   * instance.
   */
  @Override
  @SuppressWarnings("CloneDoesntCallSuperClone")
  public BytesWrap clone() {
    return new BytesWrap(this.data);
  }

  /**
   * Copy of {@link BytesRef#utf8ToString()}.
   *
   * @return String representation of the UTF8 bytes
   */
  @Override
  public String toString() {
    String str = intern.get(this.data);
    if (str == null) {
      final CharsRef ref = new CharsRef(this.data.length);
      UnicodeUtil.UTF8toUTF16(this.data, 0, this.data.length, ref);
      intern.put(this.data, ref.toString());
    }
    str = intern.get(this.data);
    if (str == null) {
      LOG.error("String was null. bytes={} bytes_size={}", this.data,
              this.data.length);
      throw new IllegalStateException("String was null.");
    }
    return str;
  }

  /**
   * Returns a reference to the internal used or referenced byte array. Meant
   * to be used in serialization.
   *
   * @return Internal used or referenced byte array
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public byte[] getBytes() {
    return this.data;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !(o instanceof BytesWrap)) {
      return false;
    }
    return Arrays.equals(this.data, ((BytesWrap) o).data);
  }

  @Override
  public int hashCode() {
    if (this.data == null) {
      throw new IllegalStateException("Data was null!");
    }
    if (this.hash == null) {
      LOG.warn("Recalc hash..");
      this.hash = Arrays.hashCode(this.data);
      if (this.hash == null) {
        throw new IllegalStateException("Hash was null!");
      }
    }
    return this.hash;
  }

  @Override
  @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
  public int compareTo(final BytesWrap o) {
    // same object, same array values
    if (this == o) {
      return 0;
    }

    // lexicographic version
    final int minSameSize = Math.min(data.length, o.data.length);
    int a, b;
    for (int i = 0; i < minSameSize; i++) {
      a = (this.data[i] & 0xff);
      b = (o.data[i] & 0xff);
      if (a != b) {
        return a - b;
      }
    }

//    final int minSameSize = Math.min(data.length, o.data.length);
//    for (int i = 0; i < minSameSize; i++) {
//      final int cmp = this.data[i] - o.data[i];
//      if (cmp != 0) {
//        return cmp;
//      }
//    }
    return data.length - o.data.length;
  }

  /**
   * Custom {@link Serializer} for {@link BytesWrap} objects.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Serializer implements
          org.mapdb.Serializer<BytesWrap>, Serializable {

    /**
     * Serialization class version id.
     */
    private static final long serialVersionUID = 0L;

    @Override
    public void serialize(final DataOutput out, final BytesWrap value) throws
            IOException {
      final byte[] bytes = value.getBytes();
      DataOutput2.packInt(out, bytes.length);
      out.write(bytes);
    }

    @Override
    public BytesWrap deserialize(final DataInput in, final int available)
            throws
            IOException {
      if (available == 0) {
        return null;
      }
      final byte[] bytes = new byte[DataInput2.unpackInt(in)];
      in.readFully(bytes);
      return new BytesWrap(bytes);
    }

    @Override
    public int fixedSize() {
      return -1;
    }
  }

  private static class InternMap<K, V> extends LinkedHashMap<K, V> {

    private final int maxSize;

    public InternMap(int maxSize) {
      this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() > maxSize;
    }
  }
}
