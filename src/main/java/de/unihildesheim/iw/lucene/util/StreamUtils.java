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

package de.unihildesheim.iw.lucene.util;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterator.OfInt;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class StreamUtils {

  /**
   * Stream contents of a {@link BytesRefArray}. Item order is not preserved.
   *
   * @param bra BytesRefArray
   * @return Stream of array content
   */
  public static Stream<BytesRef> stream(final BytesRefArray bra) {
    if (bra == null) {
      throw new IllegalArgumentException("BytesRefArray was null.");
    }
    return StreamSupport.stream(new BytesRefArraySpliterator(bra), false);
  }

  /**
   * Stream contents of a {@link TermsEnum}.
   *
   * @param te TermsEnum
   * @return Stream of enums content
   */
  public static Stream<BytesRef> stream(final TermsEnum te) {
    if (te == null) {
      throw new IllegalArgumentException("TermsEnum was null.");
    }
    return StreamSupport.stream(new TermsEnumSpliterator(te), false);
  }

  /**
   * Stream contents of a {@link BytesRefHash}.
   *
   * @param brh Hash
   * @return Stream of hashs content
   */
  public static Stream<BytesRef> stream(final BytesRefHash brh) {
    if (brh == null) {
      throw new IllegalArgumentException("BytesRefHash was null.");
    }
    return StreamSupport.stream(new BytesRefHashSpliterator(brh), false);
  }

  /**
   * Stream contents of a {@link DocIdSet}.
   *
   * @param dis DocIdSet
   * @return Stream of sets content
   * @throws IOException Thrown on low-level i/o-errors
   */
  public static IntStream stream(final DocIdSet dis)
      throws IOException {
    if (dis == null) {
      throw new IllegalArgumentException("DocIdSet was null.");
    }
    return StreamSupport.intStream(new DocIdSetSpliterator(dis), false);
  }

  /**
   * Stream contents of a {@link DocIdSetIterator}.
   *
   * @param disi DocIdSetIterator
   * @return Stream of sets content
   */
  public static IntStream stream(final DocIdSetIterator disi) {
    if (disi == null) {
      return IntStream.empty();
    } else {
      return StreamSupport.intStream(new DocIdSetSpliterator(disi), false);
    }
  }

  /**
   * Stream contents of a {@link BitSet}.
   * @param bs BitSet
   * @return Stream of active (set) bits in set
   */
  public static IntStream stream(final BitSet bs) {
    if (bs == null) {
      throw new IllegalArgumentException("BitSet was null");
    }
    return StreamSupport.intStream(new BitSetSpliterator(bs), false);
  }

  /**
   * Stream contents of a {@link Bits} instance.
   * @param b Bits
   * @return Stream of active (set) bits
   */
  public static IntStream stream(final Bits b) {
    if (b == null) {
      throw new IllegalArgumentException("Bits were null");
    }
    return StreamSupport.intStream(new BitsSpliterator(b), false);
  }

  /**
   * Spliterator over contents of a {@link FixedBitSet}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class BitSetSpliterator
  implements OfInt {
    /**
     * Wrapped {@link BitSet} instance.
     */
    private final BitSet bs;
    /**
     * Current index in wrapped instance.
     */
    private int idx = -1;

    /**
     * Creates a new {@link Spliterator} using the contents of the provided
     * {@link BitSet}. Only set bits will be streamed.
     * @param bs Bits to iterate over
     */
    public BitSetSpliterator(final BitSet bs) {
      this.bs = bs;
    }

    @Nullable
    @Override
    public OfInt trySplit() {
      return null; // cannot be split
    }

    @Override
    public long estimateSize() {
      return (long) this.bs.cardinality();
    }

    @Override
    public int characteristics() {
      return Spliterator.DISTINCT | Spliterator.IMMUTABLE |
          Spliterator.NONNULL | Spliterator.SIZED | Spliterator.ORDERED;
    }

    @Override
    public boolean tryAdvance(final IntConsumer action) {
      if (++this.idx >= this.bs.length()) {
        return false;
      }
      this.idx = this.bs.nextSetBit(this.idx);
      if (this.idx == DocIdSetIterator.NO_MORE_DOCS) {
        return false;
      }
      action.accept(this.idx);
      return true;
    }
  }

  /**
   * Spliterator over contents of a {@link DocIdSet}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class DocIdSetSpliterator
      implements OfInt {
    /**
     * Wrapped {@link DocIdSetIterator} instance.
     */
    private final DocIdSetIterator disi;

    /**
     * Creates a new {@link Spliterator} using the contents of the provided
     * {@link DocIdSet}.
     * @param dis Doc-Ids to iterate over
     * @throws IOException Thrown on low-level i/o-errors
     */
    public DocIdSetSpliterator(final DocIdSet dis)
        throws IOException {
      this.disi = dis.iterator();
    }

    /**
     * Creates a new {@link Spliterator} using the contents of the provided
     * {@link DocIdSetIterator}.
     * @param disi Iteratot to wrap
     */
    public DocIdSetSpliterator(final DocIdSetIterator disi) {
      this.disi = disi;
    }

    @Override
    public boolean tryAdvance(final IntConsumer action) {
      // iterator may be null, if there are no documents
      if (this.disi == null) {
        return false;
      }
      final int doc;
      try {
        doc = this.disi.nextDoc();

        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
          return false;
        }
        action.accept(doc);

        return true;
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Nullable
    @Override
    public OfInt trySplit() {
      return null; // cannot be split
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE; // we don't know
    }

    @Override
    public int characteristics() {
      return Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator
          .NONNULL;
    }
  }

  /**
   * Spliterator over contents of a {@link BytesRefArray}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class BytesRefArraySpliterator
      implements Spliterator<BytesRef> {
    /**
     * Wrapped {@link BytesRefIterator} instance.
     */
    private final BytesRefIterator bri;
    /**
     * Number of items of wrapped iterator.
     */
    private final int size;

    /**
     * Creates a new {@link Spliterator} using the contents of the provided
     * {@link BytesRefArray}.
     * @param bra ByteRefs to iterate over
     */
    public BytesRefArraySpliterator(final BytesRefArray bra) {
      this.size = Objects.requireNonNull(bra, "Array was null.").size();
      this.bri = bra.iterator();
    }

    @Override
    public boolean tryAdvance(final Consumer<? super BytesRef> action) {
      try {
        final BytesRef term = this.bri.next();
        if (term == null) {
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
      return (long) this.size;
    }

    @Override
    public int characteristics() {
      return Spliterator.DISTINCT | Spliterator.IMMUTABLE |
          Spliterator.NONNULL | Spliterator.SIZED;
    }
  }

  /**
   * Stream contents of a {@link TermsEnum}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class TermsEnumSpliterator
      implements Spliterator<BytesRef> {
    /**
     * Wrapped {@link TermsEnum} instance.
     */
    private final TermsEnum te;

    /**
     * Creates a new {@link Spliterator} using the contents of the provided
     * {@link TermsEnum}.
     * @param te TermsEnum to iterate
     */
    public TermsEnumSpliterator(final TermsEnum te) {
      this.te = te;
    }

    @Override
    public boolean tryAdvance(
        final Consumer<? super BytesRef> action) {
      try {
        final BytesRef nextTerm = this.te.next();
        if (nextTerm == null) {
          return false;
        } else {
          action.accept(BytesRef.deepCopyOf(nextTerm));
          return true;
        }
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    @Nullable
    public Spliterator<BytesRef> trySplit() {
      return null; // no split support
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE; // we don't know
    }

    @Override
    public int characteristics() {
      return Spliterator.DISTINCT | Spliterator.IMMUTABLE |
          Spliterator.NONNULL | Spliterator.ORDERED;
    }
  }

  /**
   * Stream contents of a {@link BytesRefHash}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class BytesRefHashSpliterator
  implements Spliterator<BytesRef> {
    /**
     * Wrapped {@link BytesRefHash} instance.
     */
    private final BytesRefHash brh;
    /**
     * Size of the wrapped hash
     */
    private final int size;
    /**
     * Current index to hash.
     */
    private int idx;

    /**
     * Creates a new {@link Spliterator} over the contents of a {@link
     * BytesRefHash}.
     * @param brh Hash to spliterate
     */
    public BytesRefHashSpliterator(final BytesRefHash brh) {
      this.brh = brh;
      this.size = brh.size();
    }

    @Override
    public boolean tryAdvance(final Consumer<? super BytesRef> action) {
      if (this.idx == this.size) {
        return false;
      }
      final BytesRef term = this.brh.get(this.idx++, new BytesRef());
      action.accept(term);
      return true;
    }

    @Nullable
    @Override
    public Spliterator<BytesRef> trySplit() {
      return null; // cannot be split
    }

    @Override
    public long estimateSize() {
      return (long) this.size;
    }

    @Override
    public int characteristics() {
      return Spliterator.DISTINCT | Spliterator.IMMUTABLE |
          Spliterator.NONNULL | Spliterator.SIZED;
    }
  }

  /**
   * Spliterator over contents of a {@link FixedBitSet}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class BitsSpliterator
      implements OfInt {
    /**
     * Current index in wrapped instance.
     */
    private int idx = -1;
    /**
     * Wrapped {@link Bits} instance.
     */
    private final Bits bits;

    /**
     * Creates a new {@link Spliterator} using the contents of the provided
     * {@link Bits} instance. Only set bits will be streamed.
     * @param b Bits to iterate over
     */
    public BitsSpliterator(final Bits b) {
      this.bits = b;
    }

    @Nullable
    @Override
    public OfInt trySplit() {
      return null; // cannot be split
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE; // we don't know
    }

    @Override
    public int characteristics() {
      return Spliterator.DISTINCT | Spliterator.IMMUTABLE |
          Spliterator.NONNULL | Spliterator.SIZED | Spliterator.ORDERED;
    }

    @Override
    public boolean tryAdvance(final IntConsumer action) {
      while (++this.idx < this.bits.length()) {
        if (this.bits.get(this.idx)) {
          action.accept(this.idx);
          return true;
        }
      }
      return false;
    }
  }
}
