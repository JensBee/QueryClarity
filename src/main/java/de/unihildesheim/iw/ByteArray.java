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
    implements Comparable<ByteArray>, Serializable, Cloneable {

  /**
   * Serializer for this class.
   */
  public static final ByteArraySerializer SERIALIZER
      = new ByteArraySerializer();
  /**
   * Serialization id.
   */
  private static final long serialVersionUID = -302418892162242172L;
  /**
   * Bytes held by this instance.
   */
  public final byte[] bytes;

  /**
   * Create a new {@link ByteArray} from the given bytes taking a local copy of
   * them.
   *
   * @param existingBytes Bytes to copy
   */
  public ByteArray(final byte[] existingBytes) {
    if (Objects.requireNonNull(existingBytes).length == 0) {
      throw new IllegalArgumentException("Empty bytes given.");
    }

    this.bytes = Arrays.copyOf(existingBytes, existingBytes.length);
  }

  /**
   * Create a new {@link ByteArray} from the given bytes taking a local copy of
   * them. Bytes will be copied starting at <tt>offset</tt> (inclusive) using
   * the given <tt>length</tt>.
   *
   * @param existingBytes Bytes array to copy from
   * @param offset Start offset
   * @param length Number of bytes to copy
   */
  public ByteArray(final byte[] existingBytes, final int offset,
      final int length) {
    if (Objects.requireNonNull(existingBytes).length == 0) {
      throw new IllegalArgumentException("Empty bytes given.");
    }
    if (existingBytes.length < (offset + length)) {
      throw new IllegalArgumentException("Not enough bytes to copy.");
    }
    this.bytes = Arrays.copyOfRange(existingBytes, offset, length);
  }

  /**
   * Create a clone of this byte array.
   *
   * @return Cloned copy
   */
  @Override
  public ByteArray clone() {
    //return new ByteArray(this.bytes);
    try {
      return (ByteArray) super.clone();
    } catch (CloneNotSupportedException ex) {
      throw new IllegalStateException("Clone not supported.");
    }
  }

  @Override
  public String toString() {
    return "ByteArray: " + Arrays.toString(this.bytes);
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
    if (o == null) {
      return 1;
    }
    return Fun.BYTE_ARRAY_COMPARATOR.compare(this.bytes, o.bytes);
  }

  @Override
  public int hashCode() {
    return Hasher.BYTE_ARRAY.hashCode(this.bytes);
  }

  @Override
  public boolean equals(final Object o) {
    return this == o || !((o == null) || !(o instanceof ByteArray)) &&
        compareTo((ByteArray) o) == 0;

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
      Serializer.BYTE_ARRAY.serialize(out, value.bytes);
    }

    @Override
    public ByteArray deserialize(final DataInput in, final int available)
        throws IOException {
      return new ByteArray(
          Serializer.BYTE_ARRAY.deserialize(in, available));
    }

    @Override
    public int fixedSize() {
      return -1;
    }
  }
}
