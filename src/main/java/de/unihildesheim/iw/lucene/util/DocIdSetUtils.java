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
    if (RoaringDocIdSet.class.isInstance(dis)) {
      return ((RoaringDocIdSet) dis).cardinality();
    }

    final BitSet bits = bits(dis);
    if (bits != null) {
      return bits.cardinality();
    }

    final DocIdSetIterator disi = dis.iterator();
    if (disi == null) {
      return 0; // no matching doc
    }
    return (int) StreamUtils.stream(disi).count();
  }

  /**
   * Get the highest document id stored in the {@link DocIdSet}.
   * @param dis DocIdSet
   * @return Highest document number
   * @throws IOException Thrown on low-level i/o-errors
   */
  public static int maxDoc(final DocIdSet dis)
      throws IOException {
    final DocIdSetIterator disi = dis.iterator();
    if (disi == null) {
      return 0;
    }

    BitSet bitSet;
    bitSet = BitSetIterator.getFixedBitSetOrNull(disi);
    if (bitSet == null) {
      bitSet = BitSetIterator.getSparseFixedBitSetOrNull(disi);
    }
    if (bitSet == null) {
      bitSet = BitsUtils.Bits2FixedBitSet(dis.bits());
    }

    if (bitSet != null) {
      return bitSet.prevSetBit(bitSet.length() -1);
    }
    return StreamUtils.stream(dis).sorted().max().getAsInt();
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
        bitSet = new SparseFixedBitSet(maxDoc(dis));
        StreamUtils.stream(disi).forEach(bitSet::set);
      }
      return bitSet;
    }
  }

  /**
   * Get the number of documents stored in the {@link DocIdSet}.
   * @param dis DocIdSet
   * @return Number of doc-ids stored in the set
   * @throws IOException Thrown on low-level i/o-errors
   */
  public static int size(final DocIdSet dis)
      throws IOException {
    int docs = 0;
    if (RoaringDocIdSet.class.isInstance(dis)) {
      docs = ((RoaringDocIdSet) dis).cardinality();
    } else {
      final DocIdSetIterator disi = dis.iterator();
      if (disi != null) {
        for (int docId = disi.nextDoc();
             docId != DocIdSetIterator.NO_MORE_DOCS;
             docId = disi.nextDoc()) {
          docs++;
        }
      }
    }
    return docs;
  }
}
