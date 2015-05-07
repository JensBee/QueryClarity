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

import de.unihildesheim.iw.data.IPCCode.Parser;
import de.unihildesheim.iw.lucene.index.builder.IndexBuilder.LUCENE_CONF;
import de.unihildesheim.iw.lucene.util.BitsUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

/**
 * Slow filter to filter documents by their IPC-code.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class IPCFieldFilter
    extends Filter {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(IPCFieldFilter.class);

  /**
   * Filter to determine, if a document is valid.
   */
  private final IPCFieldFilterFunc filterFunc;
  /**
   * Reusable parser for IPC-codes.
   */
  private final Parser ipcParser;

  /**
   * Create a new instance using the defined filter to check, if a document is
   * valid. Using the passed in char as separator for main- and sub-group.
   *
   * @param separator Separator for main- and sub-group
   * @param fFunc Filter function to use
   */
  public IPCFieldFilter(
      final char separator,
      @NotNull final IPCFieldFilterFunc fFunc) {
    this.filterFunc = fFunc;
    this.ipcParser = new Parser();
    this.ipcParser.separatorChar(separator);
  }

  /**
   * Create a new instance using the defined filter to check, if a document is
   * valid. Using the {@link Parser#DEFAULT_SEPARATOR default} char as separator
   * for main- and sub-group.
   *
   * @param fFunc Filter function to use
   */
  public IPCFieldFilter(@NotNull final IPCFieldFilterFunc fFunc) {
    this(Parser.DEFAULT_SEPARATOR, fFunc);
  }

  /**
   * Create a new instance using the defined filter to check, if a document is
   * valid. Using the {@link Parser#DEFAULT_SEPARATOR default} char as separator
   * for main- and sub-group.
   *
   * @param fFunc Filter function to use
   * @param parser Parser IPC-Code parser to use
   */
  public IPCFieldFilter(
      @NotNull final IPCFieldFilterFunc fFunc,
      @NotNull final Parser parser) {
    this.filterFunc = fFunc;
    this.ipcParser = parser;
  }

  @Override
  public DocIdSet getDocIdSet(
      @NotNull final LeafReaderContext context,
      @Nullable final Bits acceptDocs)
      throws IOException {
    final LeafReader reader = context.reader();
    final int maxDoc = reader.maxDoc();
    final BitSet finalBits = new SparseFixedBitSet(maxDoc);

    if (acceptDocs == null) {
      // check all
      for (int i = 0; i < maxDoc; i++) {
        if (this.filterFunc.isAccepted(reader, i, this.ipcParser)) {
          finalBits.set(i);
        }
      }
    } else {
      final BitSet checkBits = BitsUtils.bits2BitSet(acceptDocs);
      final DocIdSetIterator disi = new BitDocIdSet(checkBits).iterator();
      int docId;
      while ((docId = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (this.filterFunc.isAccepted(reader, docId, this.ipcParser)) {
          finalBits.set(docId);
        }
      }
    }

    return new BitDocIdSet(finalBits);
  }

  @Override
  public String toString(final String field) {
    return null;
  }

  /**
   * Generic filter to check, if a document is accepted by the {@link
   * IPCFieldFilter}.
   */
  @SuppressWarnings("PublicInnerClass")
  public abstract static class IPCFieldFilterFunc {
    /**
     * Retrieve a Document by it's id from a reader.
     *
     * @param reader Reader to load a Document
     * @param docId Id of the Document to load
     * @return Document instance
     * @throws IOException Thrown on low-level i/o errors
     */
    static Document getDocument(@NotNull final IndexReader reader,
        final int docId)
        throws IOException {
      return reader.document(docId,
          Collections.singleton(LUCENE_CONF.FLD_IPC));
    }

    /**
     * Get all stored IPC-codes for the given document.
     *
     * @param doc Document whose IPC-codes to retrieve
     * @return List of IPC-codes stored for the document
     */
    static String[] getCodes(@NotNull final Document doc) {
      return doc.getValues(LUCENE_CONF.FLD_IPC);
    }

    /**
     * Get all stored IPC-codes for the given document specified by it's id.
     *
     * @param reader Reader to load a Document
     * @param docId Document whose IPC-codes to retrieve,
     * @return List of IPC-codes stored for the document
     * @throws IOException Thrown on low-level i/o errors
     */
    static String[] getCodes(@NotNull final IndexReader reader, final int docId)
        throws IOException {
      return getDocument(reader, docId).getValues(LUCENE_CONF.FLD_IPC);
    }

    /**
     * Test, if a document should be accepted.
     *
     * @param reader Current IndexReader
     * @param docId Id of the current document
     * @param ipcParser Parser for IPC-Codes
     * @return True, if document should be accepted
     * @throws IOException Thrown on low-level i/o errors
     */
    abstract boolean isAccepted(
        @NotNull final IndexReader reader, final int docId,
        @NotNull final Parser ipcParser)
        throws IOException;

    @Override
    public abstract String toString();

    /**
     * Optional: Require to match all IPC-Codes set for a document to get
     * accepted.
     *
     * @return Self reference
     * @throws UnsupportedOperationException Thrown, if not implemented by
     * overriding class
     */
    public IPCFieldFilterFunc requireAllMatch() {
      throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Optional: Require only a single IPC-Code to match from all codes set for
     * a document to get accepted.
     *
     * @return Self reference
     * @throws UnsupportedOperationException Thrown, if not implemented by
     * overriding class
     */
    public IPCFieldFilterFunc requireSingleMatch() {
      throw new UnsupportedOperationException("Not implemented");
    }
  }

  /**
   * Wrapper to chain multiple {@link IPCFieldFilterFunc} instances.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class IPCFieldFilterMultiFunc
      extends IPCFieldFilterFunc {
    /**
     * Filters used in chain.
     */
    private final Collection<IPCFieldFilterFunc> filters = new HashSet<>(10);

    /**
     * Add a filter to the chain.
     *
     * @param func Filter to add
     */
    public void addFunc(@NotNull final IPCFieldFilterFunc func) {
      this.filters.add(func);
    }

    @Override
    boolean isAccepted(
        @NotNull final IndexReader reader,
        final int docId,
        @NotNull final Parser ipcParser)
        throws IOException {
      for (final IPCFieldFilterFunc f : this.filters) {
        if (!f.isAccepted(reader, docId, ipcParser)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("IPCFieldFilterMultiFunc [");
      this.filters.stream()
          .forEach(f -> sb.append(f).append(' '));
      return sb.append(']').toString();
    }
  }
}
