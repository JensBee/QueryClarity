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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link DocumentModel} interface. Immutable
 * document model.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DefaultDocumentModel implements DocumentModel, Serializable {

  /**
   * Serialization class version id.
   */
  private static final long serialVersionUID = 0L;

  /**
   * Logger instance for this class.
   */
  private static final transient Logger LOG = LoggerFactory.getLogger(
          DefaultDocumentModel.class);

  /**
   * Id of the document associated with this model.
   */
  private final Integer docId;

  /**
   * Initial size of {@link DefaultDocumentModel#termFreqMap} storage, if no
   * value was supplied.
   */
  private static final int INITIAL_TERMFREQMAP_SIZE = 100;

  /**
   * Stores the document frequency for each known term.
   */
  private final Map<String, Long> termFreqMap;

  /**
   * Initial size of {@link DefaultDocumentModel#termData} storage.
   */
  private static final int INITIAL_TERMDATA_SIZE = 20;

  /**
   * Stores arbitrary key-value data for each term.
   */
  private final List<TermData<String, Number>> termData;

  /**
   * Stores the calculated overall term frequency of all terms from the index.
   */
  private long overallTermFrequency = 0L;

  /**
   * Internal constructor used to create a new {@link DocumentModel} for a
   * specific document denoted by it's Lucene document id. The
   * <code>termsCount</code> value will be used to initialize the internal data
   * store.
   *
   * @param documentId Lucene's document-id
   * @param termsCount Number of terms expected for this document
   */
  private DefaultDocumentModel(final int documentId, final int termsCount) {
    this.docId = documentId;
    this.termData = new ArrayList(termsCount);
    this.termFreqMap = new HashMap(termsCount);
  }

  /**
   * Creates a new empty DocumentModel.
   */
  public DefaultDocumentModel() {
    // empty constructor
    this.docId = null;
    this.termData = null;
    this.termFreqMap = null;
  }

  /**
   * Internal constructor used to create new instances.
   *
   * @param documentId Lucene document-id
   * @param newTermFreqMap Term->frequency mapping
   * @param newTermData Term->advanced data mapping
   */
  private DefaultDocumentModel(final int documentId,
          final Map<String, Long> newTermFreqMap,
          final List<TermData<String, Number>> newTermData) {
    this.docId = documentId;
    this.termFreqMap = newTermFreqMap;
    this.termData = newTermData;
  }

  /**
   * {@inheritDoc} Adds the given term to the list of known terms, if it's not
   * already known.
   */
  @Override
  public DocumentModel addTermData(final String term, final String key,
          final Object value) {

    final List<TermData<String, Number>> newTermData;
    if (this.termData == null) {
      newTermData = new ArrayList(INITIAL_TERMDATA_SIZE);
    } else {
      newTermData = (List) ((ArrayList) this.termData).clone();
    }
    newTermData.add(new TermData(term, key, value));
    return new DefaultDocumentModel(this.docId, this.termFreqMap, newTermData);
  }

  @Override
  public Object getTermData(final String term, final String key) {
    Number retVal = null;

    if (this.termData != null) {
      // temporary object for fast lookup (?)
      final int index = this.termData.indexOf(new TermData(term, key));

      // start searching at the first occourence of the term + key
      for (int i = index; i < this.termData.size(); i++) {
        final TermData<String, Number> data = this.termData.get(i);
        if (data != null && data.getTerm().equals(term) && data.getKey().equals(
                key)) {
          retVal = data.getValue();
          break;
        }
      }
    }

    return retVal;
  }

  @Override
  public boolean containsTerm(final String term) {
    if (term == null || this.termFreqMap == null) {
      return false;
    }
    return termFreqMap.containsKey(term);
  }

  /**
   * {@inheritDoc} Adds the given term to the list of known terms, if it's not
   * already known.
   */
  @Override
  public DocumentModel addTermFrequency(final String term,
          final long frequency) {
    final Map<String, Long> newTermFreq;
    if (this.termFreqMap == null) {
      newTermFreq = new HashMap(INITIAL_TERMFREQMAP_SIZE);
    } else {
      newTermFreq = (Map) ((HashMap) this.termFreqMap).clone();
    }

    newTermFreq.put(term, frequency);
    return new DefaultDocumentModel(this.docId, newTermFreq, this.termData);
  }

  @Override
  public int getDocId() {
    if (this.docId == null) {
      throw new IllegalStateException(
              "Document id have not been initialized.");
    }
    return this.docId;
  }

  @Override
  public DocumentModel setDocId(final int documentId) {
    return new DefaultDocumentModel(documentId, this.termFreqMap,
            this.termData);
  }

  @Override
  public long getTermFrequency() {
    if (this.overallTermFrequency == 0L && this.termFreqMap != null) {
      final Iterator<Long> termFreqIt = termFreqMap.values().iterator();
      while (termFreqIt.hasNext()) {
        final Long freq = termFreqIt.next();
        if (freq != null) {
          this.overallTermFrequency += freq;
        }
      }
    }
    return this.overallTermFrequency;
  }

  @Override
  public long getTermFrequency(final String term) {
    if (term == null) {
      throw new IllegalArgumentException("Term must not be null.");
    }

    Long value;
    if (this.termFreqMap == null) {
      value = 0L;
    } else {
      value = this.termFreqMap.get(term);
      if (value == null) {
        value = 0L;
      }
    }

    return value;
  }

  @Override
  public DocumentModel create(final int documentId, final int termsCount) {
    return new DefaultDocumentModel(documentId, termsCount);
  }
}
