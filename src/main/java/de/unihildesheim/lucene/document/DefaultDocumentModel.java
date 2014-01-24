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

import de.unihildesheim.util.Tuple;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
   * If true the model is locked and immutable.
   */
  private boolean locked = false;

  /**
   * Message to throw, if model is locked.
   */
  private static final String LOCKED_MSG = "Operation not supported. "
          + "Object is locked.";

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
  private Map<String, Long> termFreqMap;

  /**
   * Initial size of {@link DefaultDocumentModel#termData} storage.
   */
  private static final int INITIAL_TERMDATA_SIZE = 20;

  /**
   * Stores arbitrary key-value data for each term.
   */
  private List<TermData<String, Number>> termData;

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
  protected DefaultDocumentModel(final int documentId, final int termsCount) {
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
   * Internal constructor used to create new instances and de-serialize objects.
   *
   * @param documentId Lucene document-id
   * @param newTermFreqMap Term->frequency mapping
   * @param newTermData Term->advanced data mapping
   */
  protected DefaultDocumentModel(final int documentId,
          final Map<String, Long> newTermFreqMap,
          final List<TermData<String, Number>> newTermData) {
    this.docId = documentId;
    this.termFreqMap = newTermFreqMap;
    this.termData = newTermData;
  }

  /**
   * Get the internal term frequency map. Used for serialization of the
   * instance.
   *
   * @return The internal term frequency map
   */
  protected Map<String, Long> getTermFreqMap() {
    return this.termFreqMap;
  }

  /**
   * Get the internal term-data list. Used for serialization of the instance.
   *
   * @return The internal term-data list
   */
  protected List<TermData<String, Number>> getTermDataList() {
    return this.termData;
  }

  /**
   * {@inheritDoc} Adds the given term to the list of known terms, if it's not
   * already known.
   */
  @Override
  public void setTermData(final String term, final String key,
          final Number value) {
    if (this.locked) {
      throw new UnsupportedOperationException(LOCKED_MSG);
    }

    if (this.termData == null) {
      this.termData = new ArrayList(INITIAL_TERMDATA_SIZE);
    }
    this.termData.add(new TermData(term, key, value));
  }

  @Override
  public Number getTermData(final String term, final String key) {
    Number retVal = null;

    if (this.termData != null) {
      // temporary object for fast lookup (?)
      final int index = this.termData.indexOf(new TermData(term, key));

      if (index > -1) {
        // start searching at the first occourence of the term + key
        for (int i = index; i < this.termData.size(); i++) {
          final TermData<String, Number> data = this.termData.get(i);
          if (data != null && data.term.equals(term) && data.key.
                  equals(key)) {
            retVal = data.value;
            break;
          }
        }
      } else {
        // not found
        retVal = null;
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

  @Override
  public void setTermFrequency(final String term,
          final long frequency) {
    if (this.locked) {
      throw new UnsupportedOperationException(LOCKED_MSG);
    }
    if (this.termFreqMap == null) {
      this.termFreqMap = new HashMap(INITIAL_TERMFREQMAP_SIZE);
    }
    this.termFreqMap.put(term, frequency);
  }

  @Override
  public int getDocId() {
    if (this.docId == null) {
      throw new IllegalStateException("Document id have not been initialized.");
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

    Long value = null;
    if (this.termFreqMap != null) {
      value = this.termFreqMap.get(term);
    }

    if (value == null) {
      value = 0L;
    }

    return value;
  }

  @Override
  public DocumentModel create(final int documentId, final int termsCount) {
    return new DefaultDocumentModel(documentId, termsCount);
  }

  @Override
  @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DefaultDocumentModel dDocMod = (DefaultDocumentModel) o;

    if (docId == null ? dDocMod.docId != null : !docId.equals(dDocMod.docId)) {
      return false;
    }

    if (termFreqMap == null ? dDocMod.termFreqMap != null : !termFreqMap.equals(
            dDocMod.termFreqMap)) {
      return false;
    }

    if (termData == null ? dDocMod.termData != null : !termData.equals(
            dDocMod.termData)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 53 * hash + Objects.hashCode(this.docId);
    return hash;
  }

  @Override
  public void lock() {
    this.locked = true;
  }

  @Override
  public void unlock() {
    this.locked = false;
  }
}
