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
package de.unihildesheim.lucene.document;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link DocumentModel} interface.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class DefaultDocumentModel implements DocumentModel, Serializable {

  private static final long serialVersionUID = 7526472295622776147L;

  /**
   * Logger instance for this class.
   */
  private static transient final Logger LOG = LoggerFactory.getLogger(
          DefaultDocumentModel.class);

  /**
   * Id of the document associated with this model.
   */
  private int docId;
  /**
   * List of all terms known by this model. The list index is used as pointer
   * for other term related data lists.
   */
  private final List<String> termIdx;
  /**
   * Stores the document frequncy for each known term.
   */
  private final List<Long> termFreq;
  /**
   * Stores arbitrary key-value data for each term.
   */
  private final List<Map<String, Object>> termData;

  private final int initialTermDataSize = 10;

  /**
   * Creates a new DocumentModel for a specific document and the number of terms
   * in the document.
   *
   * @param documentId Lucene's document-id
   * @param termsCount Number of terms found in the document. This value is used
   * to initialize the term storage to the proper size.
   */
  public DefaultDocumentModel(final int documentId, final int termsCount) {
    this.docId = documentId;
    this.termIdx = new ArrayList(termsCount);
    this.termFreq = new ArrayList(termsCount);
    this.termData = new ArrayList(termsCount);
  }

  /**
   * Get the pointer index value for the given term. Optional adds the term to
   * the list, if it's not already there.
   *
   * @param term Term to lookup
   * @param add If true, the term will be added to the list
   * @return The pointer index of the given term or <tt>-1</tt> if it could not
   * be found and should not be added.
   */
  private int getTermIdx(final String term, final boolean add) {
    int idx = this.termIdx.indexOf(term);
    if (idx == -1 && add) {
      this.termIdx.add(term);
      idx = this.termIdx.size() - 1;
//      LOG.debug("Add term t={} idx={}", term, idx);
      // reserve space for data in other lists
      this.termFreq.add(idx, null);
      this.termData.add(idx, null);
    }
    // check error case
    if (add && idx == -1) {
      throw new IllegalStateException(
              "Error while adding new term to document model index.");
    }
    return idx;
  }

  /**
   * {@inheritDoc} Adds the given term to the list of known terms, if it's not
   * already known.
   */
  @Override
  public final void setTermData(final String term, final String key,
          final Object value) {
    final int idx = getTermIdx(term, true);
    Map<String, Object> dataMap = this.termData.get(idx);

    // create new data-map if needed
    if (dataMap == null) {
      dataMap = new HashMap(initialTermDataSize);
      this.termData.add(idx, dataMap);
    }

    dataMap.put(key, value);
  }

  @Override
  public final Object getTermData(final String term, final String key) {
    final int idx = getTermIdx(term, false);
    Object data;

    if (idx > -1) {
      final Map<String, Object> dataMap = this.termData.get(idx);

      if (dataMap != null) {
        data = dataMap.get(key);
      } else {
        data = null;
      }
    } else {
      // term not found in document model index
      data = null;
    }

    return data;
  }

  @Override
  public final <T> T getTermData(final Class<T> cls, final String term,
          final String key) {
    final Object value = getTermData(term, key);
    T returnValue;
    if (value == null) {
      returnValue = null; // NOPMD
    } else {
      if (cls.isInstance(value)) {
        returnValue = cls.cast(value);
      } else {
        throw new ClassCastException("Expected " + cls.getClass()
                + ", but got " + value.getClass() + ".");
      }
    }
    return returnValue;
  }

  @Override
  public final void clearTermData(final String key) {
    for (Map<String, Object> dataMap : this.termData) {
      if (dataMap != null) {
        dataMap.remove(key);
      }
    }
  }

  @Override
  public boolean containsTerm(final String term) {
    return termIdx.contains(term);
  }

  /**
   * {@inheritDoc} Adds the given term to the list of known terms, if it's not
   * already known.
   */
  @Override
  public final void setTermFrequency(final String term, final long frequency) {
    final int idx = getTermIdx(term, true);
    this.termFreq.add(idx, frequency);
//    LOG.debug("Set term freq t={} f={} i={} got={}", term, frequency, idx,
//            this.termFreq.get(idx));
  }

  @Override
  public final int getDocId() {
    return this.docId;
  }

  /**
   * Set the lucene document id for the document referenced by this model.
   *
   * @param documentId Lcene document id
   */
  public final void setDocId(final int documentId) {
    this.docId = documentId;
  }

  @Override
  public final long getTermFrequency() {
    long value = 0L;
    final Iterator<Long> termFreqIt = termFreq.iterator();
    while (termFreqIt.hasNext()) {
      final Long freq = termFreqIt.next();
      if (freq != null) {
        value += freq;
      }
    }
    return value;
  }

  @Override
  public final long getTermFrequency(final String term) {
    final int idx = getTermIdx(term, false);
    Long value;

    if (idx > -1) {
      value = this.termFreq.get(idx);

      if (value == null || value == 0) {
        value = 0L;
      }
    } else {
      // term not found in document model index
      value = 0L;
    }

    return value;
  }
}
