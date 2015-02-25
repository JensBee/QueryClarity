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

package de.unihildesheim.iw.lucene.index;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader.SubReaderWrapper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class FilteredDirectoryReader
    extends FilterDirectoryReader {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(FilteredDirectoryReader.class);

  private final Filter filter;

  public FilteredDirectoryReader(
      final DirectoryReader dirReader, final SubReaderWrapper wrapper,
      final Filter aFilter) {
    super(dirReader, wrapper);
    this.filter = aFilter;
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(
      final DirectoryReader dirReader) {
    return new FilteredDirectoryReader(
        dirReader, new FilteredSubReaderWrapper(), this.filter);
  }

  private class FilteredSubReaderWrapper
      extends SubReaderWrapper {

    @Override
    public AtomicReader wrap(final AtomicReader reader) {
      try {
        return new FilteredAtomicReader(reader);
      } catch (final IOException e) {
        LOG.error("Error creating wrapped filter atomic reader. " +
            "Returning the plain instance instead. {}", e);
      }
      return reader;
    }
  }

  private class FilteredAtomicReader
      extends FilterAtomicReader {

    private Bits skipDocs;

    /**
     * <p>Construct a FilterAtomicReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterAtomicReader is
     * closed.</p>
     *
     * @param aReader specified base reader.
     * @throws IOException Thrown on low-level I/O errors
     */
    FilteredAtomicReader(final AtomicReader aReader)
        throws IOException {
      super(aReader);
      applyFilter();
    }

    /**
     * Based on code from <a href="https://issues.apache
     * .org/jira/browse/LUCENE-1536">LUCENE-1536</a> by Michael McCandless
     * (CachedFilterIndexReader.java).
     *
     * @throws IOException Thrown on low-level I/O errors
     */
    private void applyFilter()
        throws IOException {
      final DocIdSetIterator keepDocs =
          FilteredDirectoryReader.this.filter.getDocIdSet(
              this.in.getContext(), this.in.getLiveDocs()).iterator();
      final long maxDoc = (long) this.in.maxDoc();

      // now pre-compile single skipDocs that folds in
      // the incoming filter (negated) and deletions
      final OpenBitSet skipDocs = new OpenBitSet(maxDoc);

      // skip all docs by default
      skipDocs.set(0L, maxDoc);

      // re-enable only those documents allowed by the filter
      while (true) {
        final int docId = keepDocs.nextDoc();
        if (docId == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        skipDocs.clear((long) docId);
      }

      this.skipDocs = skipDocs;
    }

    @Override
    public Bits getLiveDocs() {
      return this.skipDocs;
    }
  }
}
