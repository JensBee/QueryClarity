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
package de.unihildesheim.iw.lucene.util;

import de.unihildesheim.iw.ByteArray;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.Counter;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utilities for working with {@link BytesRef}s.
 *
 * @author Jens Bertram
 */
public final class BytesRefUtils {

  /**
   * Private empty constructor for utility class.
   */
  private BytesRefUtils() {
    // empty
  }

  /**
   * Create a {@link ByteArray} by copying the bytes from the given {@link
   * BytesRef}.
   *
   * @param br {@link BytesRef} to copy from
   * @return New {@link ByteArray} instance with bytes copied from the {@link
   * BytesRef}
   */
  public static ByteArray toByteArray(final BytesRef br) {
    Objects.requireNonNull(br, "BytesRef was null.");
    return new ByteArray(br.bytes, br.offset, br.length);
  }

  /**
   * Return a copy of the bytes.
   *
   * @param br {@link BytesRef} to copy bytes from
   * @return Copy of bytes from {@link BytesRef}
   */
  public static byte[] copyBytes(final BytesRef br) {
    return Arrays.copyOfRange(br.bytes, br.offset, br.length);
  }

  /**
   * Creates a new {@link BytesRef} instance by cloning the bytes from the given
   * {@link BytesRef}.
   *
   * @param ba ByteArray to copy bytes from
   * @return New BytesRef with bytes from provided ByteArray
   */
  public static BytesRef fromByteArray(final ByteArray ba) {
    Objects.requireNonNull(ba, "ByteArray was null.");
    return new BytesRef(ba.bytes.clone());
  }

  /**
   * Creates a new {@link BytesRef} instance by referencing the bytes from the
   * given {@link BytesRef}.
   *
   * @param ba ByteArray to reference bytes from
   * @return New BytesRef with bytes from provided ByteArray referenced
   */
  public static BytesRef refFromByteArray(final ByteArray ba) {
    Objects.requireNonNull(ba, "ByteArray was null.");
    return new BytesRef(ba.bytes);
  }

  /**
   * Compares the bytes contained in the {@link BytesRef} to those stored in the
   * {@link ByteArray}.
   *
   * @param br {@link BytesRef} to compare
   * @param ba {@link ByteArray} to compare
   * @return True, if both byte arrays are equal
   */
  @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
  public static boolean bytesEquals(final BytesRef br, final ByteArray ba) {
    Objects.requireNonNull(br, "BytesRef was null.");
    Objects.requireNonNull(ba, "ByteArray was null.");

    return ba.compareBytes(Arrays.copyOfRange(br.bytes, br.offset,
        br.length)) == 0;
  }

  /**
   * Collector of {@link BytesRefHash}es for usage in stream expressions.
   */
  public static final class MergingBytesRefHash {
    public final BytesRefHash hash;

    public MergingBytesRefHash() {
      this.hash = new BytesRefHash();
    }

    public int add(final BytesRef br) {
      return this.hash.add(br);
    }

    public void addAll(final BytesRefHash brh) {
      for (int i = brh.size(); i >= 0; i--) {
        add(brh.get(i, new BytesRef()));
      }
    }

    public void addAll(final MergingBytesRefHash brh) {
      for (int i = brh.hash.size(); i >= 0; i--) {
        add(brh.hash.get(i, new BytesRef()));
      }
    }

    public Stream<BytesRef> stream() {
      return streamBytesRefHash(this.hash);
    }
  }

  /**
   * Stream the contents of a {@link BytesRefHash}.
   * @param hash Hash
   * @return Stream of hash content
   */
  public static Stream<BytesRef> streamBytesRefHash(
      final BytesRefHash hash) {
    final int size = hash.size();

    return StreamSupport.stream(new Spliterator<BytesRef>() {
      /**
       * Current index to hash.
       */
      private int idx;

      @Override
      public boolean tryAdvance(final Consumer<? super BytesRef> action) {
        if (this.idx == size) {
          return false;
        }
        final BytesRef term = hash.get(this.idx++, new BytesRef());
        action.accept(term);
        return true;
      }

      @Override
      @Nullable
      public Spliterator<BytesRef> trySplit() {
        return null; // cannot be split
      }

      @Override
      public long estimateSize() {
        return (long) size;
      }

      @Override
      public int characteristics() {
        return Spliterator.DISTINCT | Spliterator.IMMUTABLE |
            Spliterator.NONNULL | Spliterator.SIZED;
      }
    }, false);
  }

  /**
   * Stream the contents of a {@link BytesRefArray}.
   * @param bArr Array
   * @return Array content stream
   */
  public static Stream<BytesRef> streamBytesRefArray(final BytesRefArray bArr) {
    final int size = bArr.size();
    final BytesRefIterator bri = bArr.iterator();

    return StreamSupport.stream(new Spliterator<BytesRef>() {
      @Override
      public boolean tryAdvance(final Consumer<? super BytesRef> action) {
        try {
          final BytesRef term = bri.next();
          if (bri == null) {
            return false;
          }
          action.accept(term);
          return true;
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      @Nullable
      public Spliterator<BytesRef> trySplit() {
        return null; // cannot be split
      }

      @Override
      public long estimateSize() {
        return (long) size;
      }

      @Override
      public int characteristics() {
        return Spliterator.DISTINCT | Spliterator.IMMUTABLE |
            Spliterator.NONNULL | Spliterator.SIZED;
      }
    }, false);
  }

  /**
   * Convert a {@link BytesRefHash} to a {@link BytesRefArray}.
   * @param bh Hash
   * @return Array
   */
  public static BytesRefArray hashToArray(final BytesRefHash bh) {
    final BytesRefArray ba = new BytesRefArray(Counter.newCounter(false));
    final BytesRef br = new BytesRef();
    for (int i = bh.size(); i >=0; i--) {
      ba.append(bh.get(i, br));
    }
    return ba;
  }

  /**
   * Convert a {@link BytesRefHash} to a Set.
   * @param bh Hash
   * @return Set
   */
  public static Set<String> hashToSet(@Nullable final BytesRefHash bh) {
    if (bh == null) {
      return null;
    }
    final Set<String> strSet = new HashSet<>(bh.size());
    final BytesRef br = new BytesRef();
    for (int i = bh.size(); i >=0; i--) {
      strSet.add(bh.get(i, br).utf8ToString());
    }
    return strSet;
  }
}
