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
package de.unihildesheim.lucene.metrics;

import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.util.BytesWrap;
import java.util.Iterator;

/**
 * Document related metrics.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DocumentMetrics {

  /**
   * Model of the current document.
   */
  private final DocumentModel docModel;

  /**
   * Initialize metrics for a specific document.
   *
   * @param documentId Document to get
   */
  public DocumentMetrics(final int documentId) {
    this.docModel = Environment.getDataProvider().getDocumentModel(
            documentId);
  }

  /**
   * Initialize metrics for a specific document.
   *
   * @param documentModel Document model to use
   */
  public DocumentMetrics(final DocumentModel documentModel) {
    if (documentModel == null) {
      throw new IllegalArgumentException("DocumentModel was null.");
    }
    this.docModel = documentModel;
  }

  /**
   * Initialize metrics for a specific document.
   *
   * @param dataProv Data provider to use
   * @param documentId Document to get
   */
  public DocumentMetrics(final IndexDataProvider dataProv,
          final int documentId) {
    if (dataProv == null) {
      throw new IllegalArgumentException("DataProvider was null.");
    }
    this.docModel = dataProv.getDocumentModel(documentId);
  }

  /**
   * Get the frequency of all terms in the document.
   *
   * @return Summed frequency of all terms in document
   */
  public Long termFrequency() {
    return this.docModel.termFrequency;
  }

  /**
   * Get the frequency of all terms in the document.
   *
   * @param documentId Id of target document
   * @return Summed frequency of all terms in document
   */
  public static Long termFrequency(final int documentId) {
    return Environment.getDataProvider().getDocumentModel(
            documentId).termFrequency;
  }

  /**
   * Get the frequency of the given term in the specific document.
   *
   * @param dm Document model
   * @param term Term whose frequency to get
   * @return Frequency of the given term in the given document
   */
  private static Long termFrequency(final DocumentModel dm,
          final BytesWrap term) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null");
    }
    final Long freq = dm.termFreqMap.get(term);
    if (freq == null) {
      return 0L;
    }
    return freq;
  }

  /**
   * Get the frequency of the given term in the specific document.
   *
   * @param term Term whose frequency to get
   * @return Frequency of the given term in the given document
   */
  public Long termFrequency(final BytesWrap term) {
    return termFrequency(this.docModel, term);
  }

  /**
   * Get the frequency of the given term in the specific document.
   *
   * @param documentId Id of target document
   * @param term Term whose frequency to get
   * @return Frequency of the given term in the given document
   */
  public static Long termFrequency(final int documentId,
          final BytesWrap term) {
    return termFrequency(Environment.getDataProvider().getDocumentModel(
            documentId), term);
  }

  /**
   * Get the number of unique terms in document.
   *
   * @param dm Document model
   * @return Number of unique terms in document
   */
  private static Long uniqueTermCount(final DocumentModel dm) {
    final Integer count = dm.termFreqMap.size();
    // check for case where are more than Integer.MAX_VALUE entries
    if (count == Integer.MAX_VALUE) {
      Long manualCount = 0L;
      Iterator<BytesWrap> termsIt = dm.termFreqMap.keySet().iterator();
      while (termsIt.hasNext()) {
        manualCount++;
        termsIt.next();
      }
      return manualCount;
    }
    return count.longValue();
  }

  /**
   * Get the number of unique terms in document.
   *
   * @return Number of unique terms in document
   */
  public Long uniqueTermCount() {
    return uniqueTermCount(this.docModel);
  }

  /**
   * Get the number of unique terms in document.
   *
   * @param documentId Id of target document
   * @return Number of unique terms in document
   */
  public static Long uniqueTermCount(final int documentId) {
    return uniqueTermCount(Environment.getDataProvider().getDocumentModel(
            documentId));
  }
}
