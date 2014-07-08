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
package de.unihildesheim.iw;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.Fun;
import org.mapdb.Hasher;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

/**
 * Wrapper object for byte arrays implementing equals, clone and serialization.
 *
 * @author Jens Bertram
 */
public final class ByteArray
    implements Comparable<ByteArray>, Serializable {

  /**
   * Static ByteArray representing a maximum value. Used for caparisons.
   */
  public static final ByteArray MAX = new ByteArray();
  /**
   * MapDB {@link Serializer} for this class instances.
   */
  public static final ByteArraySerializer SERIALIZER
      = new ByteArraySerializer();
  /**
   * MapDB {@link BTreeKeySerializer} for this class instances.
   */
  public static final ByteArrayKeySerializer SERIALIZER_BTREE =
      new ByteArrayKeySerializer();
  /**
   * Comparator for this class instances.
   */
  public static final ByteArrayComparator COMPARATOR = new
      ByteArrayComparator();
  /**
   * Serialization id.
   */
  private static final long serialVersionUID = -302418892162242172L;
  /**
   * Bytes held by this instance.
   */
  @SuppressWarnings("PublicField")
  public byte[] bytes;
  /**
   * Flag indicating, if this is a maximum value.
   */
  @SuppressWarnings("PackageVisibleField")
  boolean isMax = false;

  /**
   * Private constructor. Used for {@link #MAX} only.
   */
  private ByteArray() {
    this.isMax = true;
    this.bytes = new byte[]{(byte) 0};
  }

  /**
   * Copy constructor.
   *
   * @param toClone Instance to copy values from
   */
  public ByteArray(final ByteArray toClone) {
    this(toClone.bytes.clone());
  }

  /**
   * Create a new ByteArray from the given bytes taking a local copy of them.
   *
   * @param existingBytes Bytes to copy
   */
  public ByteArray(final byte[] existingBytes) {
    if (Objects.requireNonNull(existingBytes, "Bytes were null.").length == 0) {
      throw new IllegalArgumentException("Empty bytes given.");
    }

    this.bytes = Arrays.copyOf(existingBytes, existingBytes.length);
  }

  /**
   * Create a new ByteArray from the given bytes taking a local copy of them.
   * Bytes will be copied starting at <tt>offset</tt> (inclusive) using the
   * given <tt>length</tt>.
   *
   * @param existingBytes Bytes array to copy from
   * @param offset Start offset
   * @param length Number of bytes to copy
   */
  public ByteArray(final byte[] existingBytes, final int offset,
      final int length) {
    if (Objects.requireNonNull(existingBytes, "Bytes were null.").length == 0) {
      throw new IllegalArgumentException("Empty bytes given.");
    }

    if (existingBytes.length == 0) {
      throw new IllegalStateException(
          "Bytes length is zero. " +
              "bytes-length=" + existingBytes.length + ", " +
              "offset=" + offset + ", length=" + length);
    }
    if (length == 0) {
      throw new IllegalStateException("Length is zero. " +
          "bytes-length=" + existingBytes.length + ", " +
          "offset=" + offset + ", length=" + length);
    }
    if (length < 0) {
      throw new IllegalStateException("Length is negative. " +
          "bytes-length=" + existingBytes.length + ", " +
          "offset=" + offset + ", length=" + length);
    }
    if (offset < 0) {
      throw new IllegalStateException("Offset is negative. " +
          "bytes-length=" + existingBytes.length + ", " +
          "offset=" + offset + ", length=" + length);
    }
    if (offset > existingBytes.length) {
      throw new IllegalStateException(
          "Offset out of bounds. " +
              "bytes-length=" + existingBytes.length + ", " +
              "offset=" + offset + ", length=" + length);
    }
    if (offset + length < 0) {
      throw new IllegalStateException(
          "Offset+length is negative. " +
              "bytes-length=" + existingBytes.length + ", " +
              "offset=" + offset + ", length=" + length);
    }
    if (offset + length > existingBytes.length) {
      throw new IllegalStateException(
          "Offset+length out of bounds. " +
              "bytes-length=" + existingBytes.length + ", " +
              "offset=" + offset + ", length=" + length);
    }

    this.bytes = Arrays.copyOfRange(existingBytes, offset, length);
    assert this.bytes.length > 0;
  }

  /**
   * Compare bytes stored in this instance to the given byte array.
   *
   * @param otherBytes Bytes to compare against
   * @return {@link Comparator} result
   */
  public int compareBytes(final byte[] otherBytes) {
    return Fun.BYTE_ARRAY_COMPARATOR.compare(this.bytes, otherBytes);
  }

  @Override
  public int compareTo(
      @SuppressWarnings("NullableProblems") final ByteArray o) {
    if (this.isMax) {
      if (o.isMax) {
        return 0;
      }
      return 1;
    }
    if (o.isMax) {
      return -1;
    }
    return Fun.BYTE_ARRAY_COMPARATOR.compare(this.bytes, o.bytes);
  }

  @Override
  public int hashCode() {
    return Hasher.BYTE_ARRAY.hashCode(this.bytes);
  }

  @Override
  public boolean equals(final Object o) {
    return this == o
        || (o instanceof ByteArray && this.isMax && ((ByteArray) o).isMax)
        || (o instanceof ByteArray
        && Fun.BYTE_ARRAY_COMPARATOR.compare(
        this.bytes, ((ByteArray) o).bytes) == 0);
  }

  @Override
  public String toString() {
    return "ByteArray: " + Arrays.toString(this.bytes);
  }

  /**
   * Comparator for ByteArray instances.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class ByteArrayComparator
      implements Comparator<ByteArray>, Serializable {
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = -2948227099968496081L;

    @Override
    public int compare(final ByteArray o1, final ByteArray o2) {
      if (o1.isMax) {
        if (o2.isMax) {
          return 0;
        }
        return 1;
      }
      if (o2.isMax) {
        return -1;
      }
      return Fun.BYTE_ARRAY_COMPARATOR.compare(o1.bytes, o2.bytes);
    }
  }

  /**
   * Custom MapDB {@link Serializer} for {@link ByteArray} objects.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class ByteArraySerializer
      implements org.mapdb.Serializer<ByteArray>, Serializable {

    /**
     * Serialization id.
     */
    private static final long serialVersionUID = 7347621854527675408L;

    @Override
    public void serialize(final DataOutput out, final ByteArray value)
        throws IOException {
      assert value != null && value.bytes != null && value.bytes.length >
          0 : "ByteArray was null or empty.";
      Serializer.BYTE_ARRAY.serialize(out, value.bytes);
    }

    @Override
    public ByteArray deserialize(final DataInput in, final int available)
        throws IOException {
      final byte[] value = Serializer.BYTE_ARRAY.deserialize(in, available);
      assert value != null && value.length > 0 : "ByteArray was null or empty.";
      return new ByteArray(value);
    }

    @Override
    public int fixedSize() {
      return -1;
    }
  }

  /**
   * Custom MapDB {@link BTreeKeySerializer} for {@link ByteArray} objects.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class ByteArrayKeySerializer
      extends BTreeKeySerializer<ByteArray>
      implements Serializable {
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = 7764157372999916555L;

    @Override
    public void serialize(final DataOutput out, final int start, final int end,
        final Object[] keys)
        throws IOException {
      for (int i = start; i < end; i++) {
        SERIALIZER.serialize(out, (ByteArray) keys[i]);
      }
    }

    @Override
    public ByteArray[] deserialize(final DataInput in, final int start,
        final int end, final int size)
        throws IOException {
      final ByteArray[] ret = new ByteArray[size];
      for (int i = start; i < end; i++) {
        ret[i] = SERIALIZER.deserialize(in, -1);
      }
      return ret;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Comparator<ByteArray> getComparator() {
      return (Comparator<ByteArray>) BTreeMap.COMPARABLE_COMPARATOR;
    }
  }
}
