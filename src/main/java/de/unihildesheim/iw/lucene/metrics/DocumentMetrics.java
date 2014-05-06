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
package de.unihildesheim.iw.lucene.metrics;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.Environment;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Document related metrics. High-Level API for accessing {@link
 * IndexDataProvider} data.
 */
public final class DocumentMetrics {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      DocumentMetrics.class);

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
   * Get a document model from the {@link IndexDataProvider} set by the {@link
   * Environment}.
   *
   * @param documentId Id of the document whose model to get
   * @return Document model for the given document id
   */
  public static DocumentModel getModel(final int documentId) {
    return Environment.getDataProvider().getDocumentModel(documentId);
  }

  /**
   * Calculate the Within-document Frequency for the given term.
   *
   * @param dm Document model
   * @param term Term
   * @return Frequency
   */
  public static double wdf(final DocumentModel dm, final ByteArray term) {
    return MathUtils.log2(tf(dm, term).doubleValue() + 1) / MathUtils.log2(tf(
        dm).doubleValue());
  }

  /**
   * @param term Term whose frequency to get
   * @return Frequency value
   * @see #wdf(DocumentModel, ByteArray)
   */
  public double wdf(final ByteArray term) {
    return wdf(this.docModel, term);
  }

  /**
   * Get the frequency of all terms in the document.
   *
   * @return Summed frequency of all terms in document
   */
  public Long tf() {
    return this.docModel.termFrequency;
  }

  /**
   * @param dm Model of target document
   * @return Summed frequency of all terms in document
   * @see #tf()
   */
  public static Long tf(final DocumentModel dm) {
    return dm.termFrequency;
  }

  /**
   * @param dm Document model
   * @param term Term whose frequency to get
   * @return Frequency of the given term in the given document
   * @see #tf(ByteArray)
   */
  public static Long tf(final DocumentModel dm, final ByteArray term) {
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
  public Long tf(final ByteArray term) {
    return tf(this.docModel, term);
  }

  /**
   * Get the number of unique terms in document.
   *
   * @return Number of unique terms in document
   */
  public Long uniqueTermCount() {
    return this.docModel.termCount();
  }

  /**
   * @param dm Document model
   * @return Number of unique terms in document
   * @see #uniqueTermCount()
   */
  public static Long uniqueTermCount(final DocumentModel dm) {
    return dm.termCount();
  }

  /**
   * @param dm Document model
   * @param term Term to lookup
   * @return Relative frequency. Zero if term is not in document.
   * @see #relTf(ByteArray)
   */
  public static Double relTf(final DocumentModel dm, final ByteArray term) {
    Long tf = dm.tf(term);
    if (tf == 0) {
      return 0.0;
    }
    return tf.doubleValue() / Long.valueOf(dm.termFrequency).doubleValue();
  }

  /**
   * Get the relative term frequency for a term in the document. Calculated by
   * dividing the frequency of the given term by the frequency of all terms in
   * the document.
   *
   * @param term Term to lookup
   * @return Relative frequency. Zero if term is not in document.
   */
  public Double relTf(final ByteArray term) {
    return relTf(this.docModel, term);
  }

  /**
   * Checks, if the document contains the given term.
   *
   * @param term Term to lookup
   * @return True, if term is in document
   */
  public boolean contains(final ByteArray term) {
    return this.docModel.contains(term);
  }

  /**
   * @param dm Document model
   * @param term Term to lookup
   * @return True, if term is in document
   * @see #contains(ByteArray)
   */
  public static boolean contains(final DocumentModel dm, final ByteArray term) {
    return dm.contains(term);
  }

  /**
   * @param dm Document model
   * @param term Term to lookup
   * @param smoothing Smoothing parameter
   * @return Smoothed relative term frequency
   * @see #smoothedRelativeTermFrequency(ByteArray, double)
   */
  public static double smoothedRelativeTermFrequency(
      final DocumentModel dm, final ByteArray term,
      final double smoothing) {
    final double termFreq = dm.tf(term).doubleValue();
    final double relCollFreq = relTf(dm, term);
    return ((termFreq + smoothing) * relCollFreq) / (termFreq + (Integer.
        valueOf(dm.termFreqMap.size()).doubleValue() * smoothing));
  }

  /**
   * Get the relative term frequency for a term in the document. Calculated
   * using Bayesian smoothing using Dirichlet priors.
   *
   * @param term Term to lookup
   * @param smoothing Smoothing parameter
   * @return Smoothed relative term frequency
   */
  public double smoothedRelativeTermFrequency(final ByteArray term,
      final double smoothing) {
    return smoothedRelativeTermFrequency(this.docModel, term, smoothing);
  }
}
