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

package de.unihildesheim.iw.lucene.search;

import de.unihildesheim.iw.lucene.util.BitsUtils;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Slow filter sorting out documents with no content in an specific field.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class EmptyFieldFilter
    extends Filter {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(EmptyFieldFilter.class);
  private final String field;
  private final boolean negate;

  /**
   * @param field the field to filter
   */
  public EmptyFieldFilter(final String field) {
    this(field, false);
  }

  /**
   * @param field the field to filter
   * @param negate iff {@code true} all documents with no terms in the
   * given field are accepted.
   */
  @SuppressWarnings("BooleanParameter")
  public EmptyFieldFilter(final String field, final boolean negate) {
    this.field = field;
    this.negate = negate;
  }

  /**
   * Returns the field this filter is applied on.
   *
   * @return the field this filter is applied on.
   */
  public String field() {
    return this.field;
  }

  /**
   * Returns <code>true</code> iff this filter is negated, otherwise
   * <code>false</code>
   *
   * @return <code>true</code> iff this filter is negated, otherwise
   * <code>false</code>
   */
  public boolean negate() {
    return this.negate;
  }

  @Override
  public DocIdSet getDocIdSet(
      final LeafReaderContext context, @Nullable final Bits acceptDocs)
      throws IOException {
    BitSet checkBits;
    final LeafReader reader = context.reader();
    final int maxDoc = reader.maxDoc();

    BitSet finalBits = new SparseFixedBitSet(maxDoc);
    if (acceptDocs == null) {
      checkBits = BitsUtils.bits2BitSet(reader.getLiveDocs());
      if (checkBits == null) {
        // all live
        checkBits = new FixedBitSet(maxDoc);
        ((FixedBitSet) checkBits).set(0, checkBits.length());
      }
    } else {
      checkBits = BitsUtils.bits2BitSet(acceptDocs);
    }

    @Nullable final Terms terms = reader.terms(this.field);
    if (terms != null) {
      final int termsDocCount = terms.getDocCount();

      if (termsDocCount == 0) {
        // none matching
      } else if (termsDocCount == maxDoc) {
        // all matching
        finalBits = checkBits;
      } else {
        @Nullable final Terms t = reader.terms(this.field);
        if (t != null) {
          DocsEnum de = null;
          final TermsEnum te = t.iterator(null);
          while (te.next() != null) {
            de = te.docs(checkBits, de, DocsEnum.FLAG_NONE);
            while (true) {
              final int docId = de.nextDoc();
              if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                break;
              }
              if (checkBits.get(docId)) {
                finalBits.set(docId);
                checkBits.clear(docId);
              }
            }
          }
        }
      }
    }
    return new BitDocIdSet(finalBits);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final EmptyFieldFilter that = (EmptyFieldFilter) o;

    return this.negate == that.negate &&
        !(this.field != null ? !this.field.equals(that.field) :
            that.field != null);
  }

  @Override
  public int hashCode() {
    final int result = this.field != null ? this.field.hashCode() : 0;
    return 31 * result + (this.negate ? 1 : 0);
  }

  @Override
  public String toString() {
    return "EmptyFieldFilter [field=" + this.field +
        ", negate=" + this.negate + ']';
  }
}
