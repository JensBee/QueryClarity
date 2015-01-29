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
import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.util.BigDecimalCache;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

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
  /**
   * Default math context for model calculations.
   */
  private static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf().getString(
          DefaultKeys.MATH_CONTEXT.toString()));
  /**
   * DataProvider to access statistical data.
   */
  private final IndexDataProvider dataProv;
  /**
   * Cache for created {@link DocumentModel}s.
   */
  private final Map<Integer, DocumentModel> c_docModel = DBMaker
      .newMemoryDirectDB()
      .transactionDisable()
      .make()
      .createHashMap("cache")
      .keySerializer(Serializer.INTEGER)
      .valueSerializer(Serializer.JAVA)
      .expireStoreSize(1000d)
      .make();

  /**
   * Creates a new instance using the provided DataProvider for statistical
   * data.
   *
   * @param dataProvider Provider for statistical data
   */
  public Metrics(final IndexDataProvider dataProvider) {
    this.dataProv = Objects.requireNonNull(dataProvider,
        "IndexDataProvider was null.");
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
   * @throws DataProviderException Forwarded from lower-level
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
   * @throws DataProviderException Forwarded from lower-level
   */
  public DocumentModel getDocumentModel(final int documentId)
      throws DataProviderException {
    DocumentModel d = this.c_docModel.get(documentId);
    if (d == null) {
      d = this.dataProv.getDocumentModel(documentId);
      this.c_docModel.put(documentId, d);
    }
    return d;
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
    /*public BigDecimal wdf(final ByteArray term) {
      return BigDecimalMath.log(
          BigDecimal.ONE.add(BigDecimalCache.get(tf(term)), MATH_CONTEXT))
          .divide(MathUtils.BD_LOG2, MATH_CONTEXT)
          .divide(
              BigDecimalMath.log(BigDecimalCache.get(tf()))
                  .divide(MathUtils.BD_LOG2, MATH_CONTEXT), MATH_CONTEXT);
    }*/

    /**
     * Get the frequency of the given term in the specific document.
     *
     * @param term Term whose frequency to get
     * @return Frequency of the given term in the given document
     */
    public Long tf(final ByteArray term) {
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
      return this.docModel.contains(term);
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
      final Long tf = this.docModel.tf(term);
      if (tf == 0L) {
        return BigDecimal.ZERO;
      }
      return BigDecimalCache.get(tf).divide(
          BigDecimalCache.get(this.docModel.termFrequency), MATH_CONTEXT);
    }
  }
}
