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

package de.unihildesheim.iw.lucene.index.termfilter;

import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.util.BitsUtils;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.stream.StreamSupport;

/**
 * Common terms term-filter. Skips terms exceeding a defined document
 * frequency threshold.
 */
@SuppressWarnings("PublicInnerClass")
public final class CommonTermsFilter
    extends TermFilter {
  /**
   * Common terms collected so ctx.
   */
  private final BytesRefHash commonTerms = new BytesRefHash();
  /**
   * Document frequency threshold.
   */
  private final double t;
  /**
   * Number of documents available in the index.
   */
  private double docCountDiv;
  /**
   * Array of sub-readers from top-level having at least one document.
   */
  @Nullable
  private LeafReader[] subReaders;
  /**
   * Number of sub-readers used by top-level.
   */
  private int subReaderCount;
  /**
   * Bits set for documents to check.
   */
  @Nullable
  private FixedBitSet checkBits;
  /**
   * Pre calculated threshold. While exceeding the terms are checked for being
   * common.
   */
  private int limit;
  /**
   * True, the underlying reader has no postings (is empty).
   */
  private boolean isEmpty;

  /**
   * New instance using a given threshold. The threshold value {@code t} is
   * calculated by {@code t = document_frequency(term) /
   * number_of_documents}.
   *
   * @param threshold Document frequency Threshold. If exceeded a term will be
   * marked as being too common.
   */
  public CommonTermsFilter(final double threshold) {
    this.t = threshold;
  }

  /**
   * Count the number of documents visible.
   */
  void countDocs() {
    assert this.topReader != null;
    final int docCount = this.topReader.numDocs();
    if (docCount <= 0) {
      this.isEmpty = true;
      this.docCountDiv = 0d;
      this.limit = 0;
    } else {
      this.docCountDiv = 1.0 / (double) docCount;
      this.limit = (int) Math.floor((double) docCount * this.t);
    }
  }

  /**
   * Checks, if the current frequency value will be accepted by the current
   * threshold value.
   *
   * @param docFreq Frequency value
   * @return True, if accepted, false otherwise
   */
  private boolean isAccepted(final int docFreq) {
    return ((double) docFreq * this.docCountDiv) <= this.t;
  }

  @Override
  public void setTopReader(@NotNull final IndexReader reader) {
    super.setTopReader(reader);
    assert this.topReader != null;
    if (FilteredDirectoryReader.class.isInstance(this.topReader)) {
      this.subReaders = ((FilteredDirectoryReader) this.topReader)
          .getSubReaders().stream()
              // skip readers without documents
          .filter(r -> r.numDocs() > 0)
          .toArray(LeafReader[]::new);
    } else {
      this.subReaders = this.topReader.getContext().leaves().stream()
          .map(LeafReaderContext::reader)
          .filter(r -> r.numDocs() > 0)
          .toArray(LeafReader[]::new);
    }
    this.subReaderCount = this.subReaders.length;
    this.checkBits = BitsUtils.bits2FixedBitSet(
        MultiFields.getLiveDocs(this.topReader));
    if (this.checkBits == null) {
      // all documents are live
      this.checkBits = new FixedBitSet(this.topReader.maxDoc());
      this.checkBits.set(0, this.checkBits.length());
    }
    countDocs();
  }

  @Override
  public boolean isAccepted(
      @Nullable final TermsEnum termsEnum,
      @NotNull final BytesRef term)
      throws IOException {
    if (this.topReader == null) {
      // pass through all terms at initialization time
      return true;
    }

    if (this.isEmpty || this.commonTerms.find(term) > -1) {
      return false;
    }

    PostingsEnum pe = null;
    TermsEnum te = null;
    assert this.checkBits != null;
    final FixedBitSet hitBits = new FixedBitSet(this.checkBits.length());
    final FixedBitSet checkBits = this.checkBits.clone();
    int count = this.limit;

    for (int i = this.subReaderCount - 1; i >= 0; i--) {
      assert this.subReaders != null;
      final Fields fInstance = this.subReaders[i].fields();
      final String[] fields;
      fields = FilteredDirectoryReader.FilteredFields.class.isInstance(fInstance) ?
          ((FilteredDirectoryReader.FilteredFields) fInstance).getFields() :
          StreamSupport.stream(fInstance.spliterator(), false)
              .toArray(String[]::new);
      final int fieldCount = fields.length;
      for (int j = fieldCount - 1; j >= 0; j--) {
        @Nullable final Terms t;
        t = FilteredDirectoryReader.FilteredFields.class.isInstance(fInstance) ?
            ((FilteredDirectoryReader.FilteredFields) fInstance).originalTerms(fields[j]) :
            fInstance.terms(fields[j]);

        if (t != null) {
          te = t.iterator(te);

          if (te.seekExact(term)) {
            // check, if threshold is exceeded
            if (!isAccepted(te.docFreq())) {
              this.commonTerms.add(term);
              return false;
            }

            pe = te.postings(checkBits, pe);

            int docId;
            while ((docId = pe.nextDoc()) !=
                DocIdSetIterator.NO_MORE_DOCS) {
              if (!hitBits.getAndSet(docId)) {
                // new doc
                checkBits.clear(docId);
                count--;
              }
              if (count == 0 &&
                  !isAccepted(hitBits.cardinality())) {
                this.commonTerms.add(term);
                return false;
              }
            }
          }
        }
      }
    }
    // check, if threshold is exceeded
    if (isAccepted(hitBits.cardinality())) {
      return true;
    }

    this.commonTerms.add(term);
    return false;
  }
}
