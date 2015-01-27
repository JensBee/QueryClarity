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

package de.unihildesheim.iw.lucene.scoring.data;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * Base (no-operation) class for more specific {@link VocabularyProvider}
 * implementations. Overriding implementations should replace methods as
 * needed.
 *
 * @author Jens Bertram
 */
public abstract class AbstractVocabularyProvider<T extends VocabularyProvider>
    implements VocabularyProvider {
  /**
   * Data provider for index data.
   */
  protected IndexDataProvider dataProv;
  /**
   * Filter object to filter the returned vocabulary.
   */
  protected Filter filter;
  /**
   * Document id's whose terms should be used as vocabulary.
   */
  protected Set<Integer> docIds;

  @Override
  public T indexDataProvider(final IndexDataProvider indexDataProvider) {
    this.dataProv = Objects.requireNonNull(indexDataProvider);
    return getThis();
  }

  @Override
  public T filter(final Filter filter) {
    this.filter = Objects.requireNonNull(filter);
    return getThis();
  }

  /**
   * Get a self reference of the overriding class.
   *
   * @return Self reference
   */
  public abstract T getThis();

  @Override
  public T documentIds(final Set<Integer> documentIds) {
    this.docIds = Objects.requireNonNull(documentIds);
    return getThis();
  }

  /**
   * Iterator wrapping another terms iterator and provides basic term filtering.
   */
  public static class FilteredTermsIterator
      implements Iterator<ByteArray> {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(FilteredTermsIterator.class);
    /**
     * Filter for excluding terms.
     */
    private final Filter filter;
    /**
     * Original terms iterator.
     */
    private final Iterator<ByteArray> termsIt;
    /**
     * Next term in iteration.
     */
    private ByteArray nextTerm;
    /**
     * Current iteration term.
     */
    private ByteArray term;

    /**
     * Initialize the iterator.
     * @param iterator Original iterator
     * @param termFilter Filter to exclude terms
     * @throws DataProviderException Forwarded from filter
     */
    public FilteredTermsIterator(
        final Iterator<ByteArray> iterator, final Filter termFilter)
        throws DataProviderException {
      this.filter = Objects.requireNonNull(termFilter);
      this.termsIt = Objects.requireNonNull(iterator);
      setNext();
    }

    /**
     * Get the next iteration element.
     * @throws DataProviderException Forwarded from filter
     */
    private void setNext()
        throws DataProviderException {
      this.nextTerm = null;
      while (this.termsIt.hasNext() && this.nextTerm == null) {
        this.nextTerm = this.filter.filter(this.termsIt.next());
      }
    }

    @Override
    public boolean hasNext() {
      return this.nextTerm != null;
    }

    @Override
    public ByteArray next() {
      this.term = this.nextTerm;
      if (this.term == null) {
        throw new NoSuchElementException();
      }
      try {
        setNext();
      } catch (DataProviderException e) {
        LOG.error("Failed to get next term.", e);
      }
      return term;
    }
  }
}
