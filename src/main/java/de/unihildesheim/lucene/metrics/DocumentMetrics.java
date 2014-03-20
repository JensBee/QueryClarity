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
   * Get a document model from the {@link IndexDataProvider} set by the
   * {@link Environment}.
   *
   * @param documentId Id of the document whose model to get
   * @return Document model for the given document id
   */
  private static DocumentModel getModel(final int documentId) {
    return Environment.getDataProvider().getDocumentModel(documentId);
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
    return getModel(documentId).termFrequency;
  }

  /**
   * Get the frequency of the given term in the specific document.
   *
   * @param dm Document model
   * @param term Term whose frequency to get
   * @return Frequency of the given term in the given document
   */
  public static Long termFrequency(final DocumentModel dm,
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
   * @see #termFrequency(DocumentModel, BytesWrap)
   * @param term Term whose frequency to get
   * @return Frequency of the given term in the given document
   */
  public Long termFrequency(final BytesWrap term) {
    return termFrequency(this.docModel, term);
  }

  /**
   * @see #termFrequency(DocumentModel, BytesWrap)
   * @param documentId Id of target document
   * @param term Term whose frequency to get
   * @return Frequency of the given term in the given document
   */
  public static Long termFrequency(final int documentId,
          final BytesWrap term) {
    return termFrequency(getModel(documentId), term);
  }

  /**
   * Get the number of unique terms in document.
   *
   * @return Number of unique terms in document
   */
  public Long termCount() {
    return this.docModel.termCount();
  }

  /**
   * Get the number of unique terms in document.
   *
   * @param documentId Id of target document
   * @return Number of unique terms in document
   */
  public static Long uniqueTermCount(final int documentId) {
    return getModel(documentId).termCount();
  }

  /**
   * Get the relative term frequency for a term in the document. Calculated by
   * dividing the frequency of the given term by the frequency of all terms in
   * the document.
   *
   * @param dm Document model
   * @param term Term to lookup
   * @return Relative frequency. Zero if term is not in document.
   */
  public static double relativeTermFrequency(final DocumentModel dm,
          final BytesWrap term) {
    Long tf = dm.termFrequency(term);
    if (tf == null) {
      return 0.0;
    }
    return tf.doubleValue() / Long.valueOf(dm.termFrequency).doubleValue();
  }

  /**
   * @see #relativeTermFrequency(DocumentModel, BytesWrap)
   * @param term Term to lookup
   * @return Relative frequency. Zero if term is not in document.
   */
  public double relativeTermFrequency(final BytesWrap term) {
    return relativeTermFrequency(this.docModel, term);
  }

  /**
   * @see #relativeTermFrequency(DocumentModel, BytesWrap)
   * @param documentId Id of target document
   * @param term Term to lookup
   * @return Relative frequency. Zero if term is not in document.
   */
  public static double relativeTermFrequency(final int documentId,
          final BytesWrap term) {
    return relativeTermFrequency(getModel(documentId), term);
  }

  /**
   * Checks, if the document contains the given term.
   *
   * @param term Term to lookup
   * @return True, if term is in document
   */
  public boolean contains(final BytesWrap term) {
    return this.docModel.contains(term);
  }

  /**
   * Checks, if the document contains the given term.
   *
   * @param documentId Id of target document
   * @param term Term to lookup
   * @return True, if term is in document
   */
  public static boolean contains(final int documentId, final BytesWrap term) {
    return getModel(documentId).contains(term);
  }

  /**
   * Get the relative term frequency for a term in the document. Calculated
   * using Bayesian smoothing using Dirichlet priors.
   *
   * @param dm Document model
   * @param term Term to lookup
   * @param smoothing Smoothing parameter
   * @return Smoothed relative term frequency
   */
  public static double smoothedRelativeTermFrequency(
          final DocumentModel dm, final BytesWrap term,
          final double smoothing) {
    final double termFreq = dm.termFrequency(term).doubleValue();
    final double relCollFreq = relativeTermFrequency(dm, term);
    return ((termFreq + smoothing) * relCollFreq) / (termFreq + (Integer.
            valueOf(dm.termFreqMap.size()).doubleValue() * smoothing));
  }

  /**
   * @see #smoothedRelativeTermFrequency(DocumentModel, BytesWrap, double)
   * @param term Term to lookup
   * @param smoothing Smoothing parameter
   * @return Smoothed relative term frequency
   */
  public double smoothedRelativeTermFrequency(final BytesWrap term,
          final double smoothing) {
    return smoothedRelativeTermFrequency(this.docModel, term, smoothing);
  }

  /**
   * @see #smoothedRelativeTermFrequency(DocumentModel, BytesWrap, double)
   * @param documentId Id of target document
   * @param term Term to lookup
   * @param smoothing Smoothing parameter
   * @return Smoothed relative term frequency
   */
  public static double smoothedRelativeTermFrequency(final int documentId,
          final BytesWrap term, final double smoothing) {
    return smoothedRelativeTermFrequency(getModel(documentId), term,
            smoothing);
  }

}
