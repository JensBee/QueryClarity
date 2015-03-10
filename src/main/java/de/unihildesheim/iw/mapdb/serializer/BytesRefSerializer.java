/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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

package de.unihildesheim.iw.mapdb.serializer;

import de.unihildesheim.iw.ByteArray;
import org.apache.lucene.util.BytesRef;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

/**
 * MapDB serializers, comparators, etc. for {@link BytesRef} objects.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class BytesRefSerializer {
  /**
   * MapDB {@link Serializer} for {@link BytesRef} objects.
   */
  public static final org.mapdb.Serializer<BytesRef> SERIALIZER = new
      Serializer();
  /**
   * MapDB {@link BTreeKeySerializer} for {@link BytesRef} objects.
   */
  public static final BytesRefKeySerializer SERIALIZER_BTREE =
      new BytesRefKeySerializer();
  /**
   * Comparator for {@link BytesRef} instances.
   */
  public static final Comparator<BytesRef> COMPARATOR =
      BytesRef.getUTF8SortedAsUnicodeComparator();

  /**
   * Custom MapDB {@link Serializer} for {@link BytesRef} objects. Does not
   * handle n{@code null} values.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Serializer
      implements org.mapdb.Serializer<BytesRef>, Serializable {
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = 7347621854527675408L;

    @Override
    public void serialize(final DataOutput out, final BytesRef value)
        throws IOException {
      if (value == null) {
        throw new NullPointerException("BytesRef was null.");
      }
      final byte[] bytes = Arrays
          .copyOfRange(value.bytes, value.offset, value.offset + value.length);
      org.mapdb.Serializer.BYTE_ARRAY.serialize(out, bytes);
    }

    @Override
    public BytesRef deserialize(final DataInput in, final int available)
        throws IOException {
      final byte[] value = org.mapdb.Serializer.BYTE_ARRAY
          .deserialize(in, available);
      return new BytesRef(value);
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
  public static final class BytesRefKeySerializer
      extends BTreeKeySerializer<BytesRef>
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
        SERIALIZER.serialize(out, (BytesRef) keys[i]);
      }
    }

    @Override
    public BytesRef[] deserialize(final DataInput in, final int start,
        final int end, final int size)
        throws IOException {
      final BytesRef[] ret = new BytesRef[size];
      for (int i = start; i < end; i++) {
        ret[i] = SERIALIZER.deserialize(in, -1);
      }
      return ret;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Comparator<BytesRef> getComparator() {
      return (Comparator<BytesRef>) BTreeMap.COMPARABLE_COMPARATOR;
    }
  }
}
