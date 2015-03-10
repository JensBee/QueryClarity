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

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.RoaringDocIdSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class DocIdSetUtils {

  /**
   * Get the count of documents available in the set.
   *
   * @param dis Documents id set
   * @return Cardinality
   * @throws IOException Thrown on low-level I/O-errors
   */
  public static int cardinality(final DocIdSet dis)
      throws IOException {
    if (dis == null) {
      throw new IllegalArgumentException("DocIdSet was null.");
    }
    final int cardinality;

    if (RoaringDocIdSet.class.isInstance(dis)) {
      cardinality = ((RoaringDocIdSet) dis).cardinality();
    } else {
      final BitSet bits = bits(dis);
      if (bits == null) {
        final DocIdSetIterator disi = dis.iterator();
        cardinality = disi == null ? 0 : (int) StreamUtils.stream(disi).count();
      } else {
        cardinality = bits.cardinality();
      }
    }
    return cardinality < 0 ? 0 : cardinality;
  }

  /**
   * Get the highest document id stored in the {@link DocIdSet}.
   *
   * @param dis DocIdSet
   * @return Highest document number or {@code -1}, if there's no document
   * @throws IOException Thrown on low-level i/o-errors
   */
  public static int maxDoc(final DocIdSet dis)
      throws IOException {
    if (dis == null) {
      throw new IllegalArgumentException("DocIdSet was null.");
    }

    final int maxDoc;

    final DocIdSetIterator disi = dis.iterator();
    if (disi == null) {
      maxDoc = 0;
    } else {
      BitSet bitSet;
      bitSet = BitSetIterator.getFixedBitSetOrNull(disi);
      if (bitSet == null) {
        bitSet = BitSetIterator.getSparseFixedBitSetOrNull(disi);
      }
      if (bitSet == null) {
        bitSet = BitsUtils.Bits2FixedBitSet(dis.bits());
      }

      if (bitSet == null) {
        maxDoc = StreamUtils.stream(dis).sorted().max().getAsInt();
      } else {
        if (bitSet.length() == 0) {
          maxDoc = -1;
        } else if (bitSet.length() == 1) {
          maxDoc = bitSet.get(0) ? 0 : -1;
        } else {
          maxDoc = bitSet.prevSetBit(bitSet.length() - 1);
        }
      }
    }
    return maxDoc;
  }

  /**
   * Get a bits instance from a DocIdSet.
   *
   * @param dis Set whose bits to get
   * @return Bits or null, if no bits are set
   * @throws IOException Thrown on low-level I/O-errors
   */
  @Nullable
  public static BitSet bits(final DocIdSet dis)
      throws IOException {
    if (dis == null) {
      throw new IllegalArgumentException("DocIdSet was null.");
    }

    final DocIdSetIterator disi = dis.iterator();

    if (disi == null) {
      return null;
    } else {
      BitSet bitSet;

      bitSet = BitSetIterator.getFixedBitSetOrNull(disi);
      if (bitSet == null) {
        bitSet = BitSetIterator.getSparseFixedBitSetOrNull(disi);
      }
      if (bitSet == null) {
        bitSet = BitsUtils.Bits2FixedBitSet(dis.bits());
      }

      if (bitSet == null) {
        bitSet = new SparseFixedBitSet(maxDoc(dis) + 1);
        StreamUtils.stream(disi).forEach(bitSet::set);
      }
      return bitSet;
    }
  }
}
