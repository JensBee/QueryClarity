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

package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.mapdb.DBMakerUtils;
import de.unihildesheim.iw.util.BigDecimalCache;
import de.unihildesheim.iw.util.MathUtils;
import org.nevec.rjm.BigDecimalMath;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Map;
import java.util.Objects;

/**
 * High level wrapper for {@link IndexDataProvider} to provide collection and
 * document related metrics. <br> Beware that {@link DocumentModel}s get cached
 * (per {@link Metrics} instance). If you change parameters that affect the
 * contents of the {@link DocumentModel}s (e.g. stopwords, document fields) you
 * should create a new {@link Metrics} instance with a fresh cache.
 *
 * @author Jens Bertram
 */
public final class Metrics {
  private static final MathContext MATH_CONTEXT = MathContext.DECIMAL128;
  /**
   * Collection related metrics are gathered in this object.
   */
  private final CollectionMetrics collection;
  /**
   * DataProvider to access statistical data.
   */
  private final IndexDataProvider dataProv;
  /**
   * Cache for created {@link DocumentModel}s.
   */
  private final Map<Integer, DocumentModel> docModelCache =
      DBMakerUtils.newCompressedCache(0.5); // size in GB

  /**
   * Creates a new instance using the provided DataProvider for statistical
   * data.
   *
   * @param dataProvider Provider for statistical data
   */
  public Metrics(final IndexDataProvider dataProvider) {
    this.dataProv = Objects.requireNonNull(dataProvider,
        "IndexDataProvider was null.");
    this.collection = new CollectionMetrics();
  }

  /**
   * Get the DataProvider used for statistical data.
   *
   * @return DataProvider in use
   */
  public IndexDataProvider getDataProvider() {
    return this.dataProv;
  }

  /**
   * Get document-metrics for the document identified by the provided Lucene
   * document-id.
   *
   * @param docId Lucene document-id of the target document
   * @return Metrics instance for the specific document
   */
  public DocumentMetrics document(final int docId)
      throws DataProviderException {
    return document(getDocumentModel(docId));
  }

  /**
   * Get document-metrics for the document identified by the Lucene document-id
   * extracted from the provided Model.
   *
   * @param docModel Document model describing the target document
   * @return Metrics instance for the specific document
   */
  public static DocumentMetrics document(final DocumentModel docModel) {
    return new DocumentMetrics(Objects.requireNonNull(docModel,
        "Document-model was null."));
  }

  /**
   * Get a document model from the {@link IndexDataProvider}.
   *
   * @param documentId Id of the document whose model to get
   * @return Document-model for the given document id
   */
  public DocumentModel getDocumentModel(final int documentId)
      throws DataProviderException {
    DocumentModel d = this.docModelCache.get(documentId);
    if (d == null) {
      d = this.dataProv.getDocumentModel(documentId);
      this.docModelCache.put(documentId, d);
    }
    return d;
  }

  /**
   * Get an object providing all collection related metrics.
   *
   * @return Collection metrics
   */
  public CollectionMetrics collection() {
    assert this.collection != null;
    return this.collection;
  }

  /**
   * High level wrapper for {@link IndexDataProvider} to provide document
   * related metrics.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class DocumentMetrics {

    /**
     * Model to retrieve data from.
     */
    private final DocumentModel docModel;

    /**
     * Create a new metrics instance for the document described by the provided
     * model.
     *
     * @param documentModel Model describing the target document
     */
    public DocumentMetrics(final DocumentModel documentModel) {
      this.docModel = Objects.requireNonNull(documentModel,
          "Document-model was null.");
    }

    /**
     * Calculate the Within-document Frequency for the given term.
     *
     * @param term Term whose frequency to get
     * @return Frequency value
     */
    public BigDecimal wdf(final ByteArray term) {
      Objects.requireNonNull(term, "Term was null.");

      return BigDecimalMath.log(
          BigDecimal.ONE.add(BigDecimalCache.get(tf(term))))
          .divide(MathUtils.BD_LOG2, MATH_CONTEXT)
          .divide(
              BigDecimalMath.log(BigDecimalCache.get(tf()))
                  .divide(MathUtils.BD_LOG2, MATH_CONTEXT)
          );

//      return MathUtils.log2(tf(term).doubleValue() + 1d) /
//          MathUtils.log2(tf().doubleValue());
    }

    /**
     * Get the frequency of the given term in the specific document.
     *
     * @param term Term whose frequency to get
     * @return Frequency of the given term in the given document
     */
    public Long tf(final ByteArray term) {
      Objects.requireNonNull(term, "Term was null.");
      final Long freq = this.docModel.getTermFreqMap().get(term);
      if (freq == null) {
        return 0L;
      }
      return freq;
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
     * Get the number of unique terms in document.
     *
     * @return Number of unique terms in document
     */
    public Long uniqueTermCount() {
      return this.docModel.termCount();
    }

    /**
     * Checks, if the document contains the given term.
     *
     * @param term Term to lookup
     * @return True, if term is in document
     */
    public boolean contains(final ByteArray term) {
      return this.docModel.contains(Objects.requireNonNull(term,
          "Term was null."));
    }

    /**
     * Get the relative term frequency for a term in the document. Calculated by
     * dividing the frequency of the given term by the frequency of all terms in
     * the document.
     *
     * @param term Term to lookup
     * @return Relative frequency. Zero if term is not in document.
     */
    public BigDecimal relTf(final ByteArray term) {
      Objects.requireNonNull(term, "Term was null.");
      final Long tf = this.docModel.tf(term);
      if (tf == 0L) {
        return BigDecimal.ZERO;
//        return 0d;
      }
      return BigDecimalCache.get(tf).divide(
          BigDecimalCache.get(this.docModel.termFrequency), MATH_CONTEXT);
//      return tf.doubleValue() / Long.valueOf(this.docModel.termFrequency)
//          .doubleValue();
    }
  }

  /**
   * High level wrapper for {@link IndexDataProvider} to provide collection
   * related metrics.
   */
  @SuppressWarnings("PublicInnerClass")
  public final class CollectionMetrics {
    /**
     * Get the number of unique terms in the index.
     *
     * @return Number of unique terms in index
     * @throws DataProviderException Thrown on errors retrieving values from the
     * DataProvider
     */
    public Long numberOfUniqueTerms()
        throws DataProviderException {
      return getDataProvider().getUniqueTermsCount();
    }

    /**
     * Get the raw frequency of a given term in the collection.
     *
     * @param term Term to lookup
     * @return Collection frequency of the given term
     */
    public Long tf(final ByteArray term)
        throws DataProviderException {
      return getDataProvider()
          .getTermFrequency(Objects.requireNonNull(term, "Term was null."));
    }

    /**
     * Get the overall raw term frequency of all terms in the index.
     *
     * @return Collection term frequency
     */
    public Long tf()
        throws DataProviderException {
      return getDataProvider().getTermFrequency();
    }

    /**
     * Get the relative frequency of a term. The relative frequency is the
     * frequency {@code tF} of term {@code t}t divided by the frequency {@code
     * F} of all terms.
     *
     * @param term Term to lookup
     * @return Relative collection frequency of the given term
     */
    public BigDecimal relTf(final ByteArray term)
        throws DataProviderException {
      return getDataProvider()
          .getRelativeTermFrequency(Objects.requireNonNull(term,
              "Term was null."));
    }

    /**
     * Calculate the inverse document frequency (IDF) using a logarithmic base
     * of 10.
     *
     * @param term Term to calculate
     * @return Inverse document frequency (log10)
     */
    public BigDecimal idf(final ByteArray term)
        throws DataProviderException {
      return idf(term, 10d);
    }

    /**
     * Calculate the inverse document frequency (IDF) using a custom logarithmic
     * base value.
     *
     * @param term Term to calculate
     * @param logBase Logarithmic base
     * @return Inverse document frequency (logN)
     */
    public BigDecimal idf(final ByteArray term, final double logBase)
        throws DataProviderException {
      Objects.requireNonNull(term, "Term was null.");

      return
          BigDecimalMath.log(BigDecimalCache.get(numberOfDocuments())
              .divide(BigDecimalCache.get(df(term)))
              .add(BigDecimal.ONE))
              .divide(BigDecimalMath.log(BigDecimalCache.get(logBase)));

//      return MathUtils.logN(logBase,
//          (1d + (numberOfDocuments().doubleValue() / df(term).doubleValue()
// )));
    }

    /**
     * Get the number of documents in the index.
     *
     * @return Number of documents in index
     */
    public Long numberOfDocuments()
        throws DataProviderException {
      return getDataProvider().getDocumentCount();
    }

    /**
     * Get the document frequency of a term.
     *
     * @param term Term to lookup.
     * @return Document frequency of the given term
     */
    public Integer df(final ByteArray term)
        throws DataProviderException {
      return getDataProvider().getDocumentFrequency(Objects.requireNonNull(term,
          "Term was null."));
    }

    /**
     * Calculate the Okapi BM25 derivation of the inverse document frequency
     * (IDF) using a logarithmic base value of 10.
     *
     * @param term Term to calculate
     * @return Inverse document frequency BM25 (logN)
     */
    public BigDecimal idfBM25(final ByteArray term)
        throws DataProviderException {
      return idfBM25(term, 10d);
    }

    /**
     * Calculate the Okapi BM25 derivation of the inverse document frequency
     * (IDF) using a custom logarithmic base value.
     *
     * @param term Term to calculate
     * @param logBase Logarithmic base
     * @return Inverse document frequency BM25 (logN)
     */
    public BigDecimal idfBM25(final ByteArray term, final double logBase)
        throws DataProviderException {
      final int docFreq = df(Objects.requireNonNull(term,
          "Term was null."));

      return
          BigDecimalMath.log(
              BigDecimalCache.get(numberOfDocuments() - docFreq + 0.5)
                  .divide(BigDecimalCache.get(docFreq + 0.5))
          ).divide(BigDecimalMath.log(BigDecimalCache.get(logBase)));

//      return MathUtils.logN(logBase,
//          (numberOfDocuments().doubleValue() - docFreq + 0.5)
//              / (docFreq + 0.5));
    }
  }
}
