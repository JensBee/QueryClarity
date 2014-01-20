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
 * Default implementation of the {@link DocumentModel} interface. Immutable
 * document model.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DefaultDocumentModel implements DocumentModel, Serializable {

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
   * Stores the document frequency for each known term.
   */
  private final Map<String, Long> termFreqMap;

  /**
   * Stores arbitrary key-value data for each term.
   */
  private final List<TermData<String, Number>> termData;

  /**
   * Creates a new DocumentModel for a specific document denoted by it's Lucene
   * document id.
   *
   * @param documentId Lucene's document-id
   */
  public DefaultDocumentModel(final int documentId) {
    this.docId = documentId;
    this.termData = null;
    this.termFreqMap = null;
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
      newTermData = new ArrayList();
    } else {
      newTermData = (List) ((ArrayList)this.termData).clone();
    }
    newTermData.add(new TermData(term, key, value));
    return new DefaultDocumentModel(this.docId, this.termFreqMap, newTermData);
  }

  @Override
  public Object getTermData(final String term, final String key) {
    Number retVal = null;

    if (this.termData != null) {
      for (TermData<String, Number> data : this.termData) {
        if (data != null && data.getTerm() != null && data.getKey() != null
                && data.getTerm().equals(term) && data.getKey().equals(key)) {
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
    return termFreqMap.keySet().contains(term);
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
      newTermFreq = new HashMap();
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
    return new DefaultDocumentModel(documentId, this.termFreqMap, this.termData);
  }

  @Override
  public long getTermFrequency() {
    long value = 0L;

    if (this.termFreqMap != null) {
      final Iterator<Long> termFreqIt = termFreqMap.values().iterator();
      while (termFreqIt.hasNext()) {
        final Long freq = termFreqIt.next();
        if (freq != null) {
          value += freq;
        }
      }
    }
    return value;
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
}
