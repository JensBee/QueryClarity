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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Byte using MapDB serializer interface.
 *
 * @author Jens Bertram
 */
public final class SerializableByte
    implements Serializable, Comparable<SerializableByte> {

  /**
   * Serializer for this class.
   */
  public static final SerializableByteSerializer SERIALIZER
      = new SerializableByteSerializer();
  /**
   * Comparator for this class.
   */
  public static final SerializableByteComparator COMPARATOR
      = new SerializableByteComparator();

  /**
   * Serialization id.
   */
  private static final long serialVersionUID = 8222282593200522446L;

  /**
   * The value.
   */
  public final byte value;

  /**
   * Create a new byte value.
   *
   * @param newValue Initial value
   */
  public SerializableByte(final byte newValue) {
    this.value = newValue;
  }

  /**
   * Copy constructor.
   *
   * @param toClone Instance to copy value from
   */
  public SerializableByte(final SerializableByte toClone) {
    this.value = toClone.value;
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public int compareTo(final SerializableByte o) {
    return Byte.compare(this.value, o.value);
  }

  @Override
  public int hashCode() {
    return (int) this.value;
  }

  @Override
  public boolean equals(final Object obj) {
    return (obj instanceof SerializableByte) &&
        (this.value == ((SerializableByte) obj).value);
  }

  @Override
  public String toString() {
    return "SerializableByte: " + Byte.toString(this.value);
  }

  /**
   * Comparator for {@link SerializableByte} objects.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SerializableByteComparator
      implements
      Comparator<SerializableByte>, Serializable {

    /**
     * Serialization id.
     */
    private static final long serialVersionUID = 6463046054600627706L;

    @Override
    public int compare(final SerializableByte o1,
        final SerializableByte o2) {
      return Byte.compare(o1.value, o2.value);
    }

  }

  /**
   * Custom MapDB {@link SerializableByteSerializer} for {@link
   * SerializableByte} objects.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SerializableByteSerializer
      implements
      org.mapdb.Serializer<SerializableByte>, Serializable {

    /**
     * Serialization id.
     */
    private static final long serialVersionUID = -3411415692071715362L;

    @Override
    public void serialize(final DataOutput out, final SerializableByte aByte)
        throws IOException {
      if (aByte == null) {
        throw new IllegalArgumentException("Value was null.");
      }
      out.writeByte((int) aByte.value);
    }

    @Override
    public SerializableByte deserialize(final DataInput in, final int available)
        throws IOException {
      return new SerializableByte(in.readByte());
    }

    @Override
    public int fixedSize() {
      return 1;
    }
  }
}
