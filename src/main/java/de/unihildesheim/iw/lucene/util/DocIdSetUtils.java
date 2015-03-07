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
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class DocIdSetUtils {

  /**
   * Get the count of documents available in the set.
   * @param dis Documents id set
   * @return Cardinality
   * @throws IOException Thrown on low-level I/O-errors
   */
  public static int cardinality(final DocIdSet dis)
      throws IOException {
    if (RoaringDocIdSet.class.isInstance(dis)) {
      return ((RoaringDocIdSet) dis).cardinality();
    }

    final FixedBitSet bits = BitsUtils.Bits2FixedBitSet(dis.bits());
    if (bits != null) {
      return bits.cardinality();
    }

    final DocIdSetIterator disi = dis.iterator();
    if (disi == null) {
      return 0; // no matching doc
    }
    int cardinality = 0;
    while (true) {
      final int docId = disi.nextDoc();
      if (docId == DocIdSetIterator.NO_MORE_DOCS) {
        break;
      }
      cardinality++;
    }
    return cardinality;
  }

  /**
   * Get a bits instance from a DocIdSet.
   * @param dis Set whose bits to get
   * @return Bits or null, if no bits are set
   * @throws IOException Thrown on low-level I/O-errors
   */
  @Nullable
  public static FixedBitSet bits(final DocIdSet dis)
      throws IOException {
    final DocIdSetIterator disi = dis.iterator();

    if (disi == null) {
      return null;
    } else {
      FixedBitSet bitSet = BitSetIterator.getFixedBitSetOrNull(disi);
      if (bitSet == null) {
        final Collection<Integer> docIds = new ArrayList<>();
        for (int docId = disi.nextDoc();
             docId != DocIdSetIterator.NO_MORE_DOCS;
             docId = disi.nextDoc()) {
          docIds.add(docId);
        }
        if (docIds.isEmpty()) {
          return null;
        }
        bitSet = new FixedBitSet(docIds.size());
        docIds.forEach(bitSet::set);
      }
      return bitSet;
    }
  }
}
