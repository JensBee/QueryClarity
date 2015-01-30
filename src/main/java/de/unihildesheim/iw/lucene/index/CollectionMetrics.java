/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collections;
import java.util.Map;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class CollectionMetrics {
  /**
   * Default math context for calculations.
   */
  private static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf().getString(
          DefaultKeys.MATH_CONTEXT.toString()));
  /**
   * Index total term frequency value.
   */
  private final BigDecimal tf;
  /**
   * Number of documents in index.
   */
  private final BigDecimal docCount;
  /**
   * Cache document frequency values.
   */
  private final Map<ByteArray, Integer> c_df;
  /**
   * Cache term frequency values.
   */
  private final Map<ByteArray, Long> c_tf;
  /**
   * Cache relative term frequency values.
   */
  private final Map<ByteArray, BigDecimal> c_rtf;
  /**
   * Cache for created {@link DocumentModel}s.
   */
  private final Map<Integer, DocumentModel> c_docModel;
  /**
   * Data provider to access index data.
   */
  private final IndexDataProvider dataProv;
  /**
   * Configuration for this class.
   */
  @NotNull
  private final CollectionMetricsConfiguration conf;

  /**
   * Configuration object for metrics class. Defaults to cache most of the
   * values retrieved from the {@link IndexDataProvider}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class CollectionMetricsConfiguration {
    /**
     * Should document frequency values be cached?
     */
    private boolean cacheDf = true;
    /**
     * Should term frequency values be cached?
     */
    private boolean cacheTf = true;
    /**
     * Should document models be cached?
     */
    private boolean cacheDocModels = true;

    /**
     * Disable caching of document frequency values.
     * @return Self reference
     */
    public CollectionMetricsConfiguration noCacheDf() {
      this.cacheDf = false;
      return this;
    }

    /**
     * Disable caching of term frequency values.
     * @return Self reference
     */
    public CollectionMetricsConfiguration noCacheTf() {
      this.cacheTf = false;
      return this;
    }

    /**
     * Disable caching of document models.
     * @return Self reference
     */
    public CollectionMetricsConfiguration noCacheDocModels() {
      this.cacheDocModels = false;
      return this;
    }
  }

  /**
   * Initialize the collection data provider using a default configuration.
   *
   * @param idp DataProvider
   * @throws DataProviderException Forwarded from lower-level
   */
  public <I extends IndexDataProvider> CollectionMetrics(final I idp)
      throws DataProviderException {
    this(idp, null);
  }

  /**
   * Initialize the collection data provider - optionally caching some basic
   * index information, based on settings made in the configuration object.
   *
   * @param idp DataProvider
   * @param cmConf Configuration
   * @throws DataProviderException Forwarded from lower-level
   */
  public <I extends IndexDataProvider> CollectionMetrics(
      final I idp,
      @Nullable final CollectionMetricsConfiguration cmConf)
      throws DataProviderException {
    this.dataProv = idp;
    // set configuration
    if (cmConf == null) {
      // default
      this.conf = new CollectionMetricsConfiguration();
    } else {
      // user supplied
      this.conf = cmConf;
    }
    this.tf = BigDecimal.valueOf(this.dataProv.getTermFrequency());
    this.docCount = BigDecimal.valueOf(this.dataProv.getDocumentCount());

    this.c_rtf = DBMaker
        .newMemoryDirectDB()
        .transactionDisable()
        .make()
        .createHashMap("cache")
        .keySerializer(ByteArray.SERIALIZER)
        .valueSerializer(Serializer.BASIC)
        .expireStoreSize(1500d)
        .make();

    if (this.conf.cacheDocModels) {
      this.c_docModel = DBMaker
          .newMemoryDirectDB()
          .transactionDisable()
          .make()
          .createHashMap("cache")
          .keySerializer(Serializer.INTEGER)
          .valueSerializer(Serializer.JAVA)
          .expireStoreSize(1000d)
          .make();
    } else {
      this.c_docModel = Collections.EMPTY_MAP;
    }

    if (this.conf.cacheDf) {
      this.c_df = DBMaker
          .newMemoryDirectDB()
          .transactionDisable()
          .make()
          .createHashMap("cache")
          .keySerializer(ByteArray.SERIALIZER)
          .valueSerializer(Serializer.INTEGER)
          .expireStoreSize(1500d)
          .make();
    } else {
      this.c_df = Collections.EMPTY_MAP;
    }

    if (this.conf.cacheTf) {
      this.c_tf = DBMaker
          .newMemoryDirectDB()
          .transactionDisable()
          .make()
          .createHashMap("cache")
          .keySerializer(ByteArray.SERIALIZER)
          .valueSerializer(Serializer.LONG)
          .expireStoreSize(1500d)
          .make();
    } else {
      this.c_tf = Collections.EMPTY_MAP;
    }
  }

  /**
   * Get the number of unique terms in the index.
   *
   * @return Number of unique terms in index
   * @throws DataProviderException Thrown on errors retrieving values from the
   * DataProvider
   */
    /*public Long numberOfUniqueTerms()
        throws DataProviderException {
      return Metrics.this.dataProv.getUniqueTermsCount();
    }*/

  /**
   * Get the raw frequency of a given term in the collection.
   *
   * @param term Term to lookup
   * @return Collection frequency of the given term
   * @throws DataProviderException Forwarded from lower-level
   */
  public Long tf(final ByteArray term) {
    if (this.conf.cacheTf) {
      Long result = this.c_tf.get(term);
      if (result == null) {
        // may return null, if term is not known
        result = this.dataProv.getTermFrequency(term);
        if (result == null) {
          return 0L;
        }
        this.c_tf.put(term, result);
      }
      return result;
    } else {
      return this.dataProv.getTermFrequency(term);
    }
  }

  /**
   * Get the overall raw term frequency of all terms in the index.
   *
   * @return Collection term frequency
   */
    /*public Long tf()
        throws DataProviderException {
      return Metrics.this.dataProv.getTermFrequency();
    }*/

  /**
   * Get the relative frequency of a term. The relative frequency is the
   * frequency {@code tF} of term {@code t}t divided by the frequency {@code F}
   * of all terms.
   *
   * @param term Term to lookup
   * @return Relative collection frequency of the given term
   * @throws DataProviderException Forwarded from lower-level
   */
  public BigDecimal relTf(final ByteArray term) {
    BigDecimal result = this.c_rtf.get(term);
    if (result == null) {
      final long tf = tf(term);
      if (tf == 0L) {
        result = BigDecimal.ZERO;
      } else {
        result = BigDecimal.valueOf(tf).divide(this.tf, MATH_CONTEXT);
      }
      this.c_rtf.put(term, result);
    }
    return result;
  }

  /**
   * Calculate the inverse document frequency (IDF) using a logarithmic base
   * of 10.
   *
   * @param term Term to calculate
   * @return Inverse document frequency (log10)
   */
    /*public BigDecimal idf(final ByteArray term)
        throws DataProviderException {
      return idf(term, 10d);
    }*/

  /**
   * Calculate the inverse document frequency (IDF) using a custom logarithmic
   * base value.
   *
   * @param term Term to calculate
   * @param logBase Logarithmic base
   * @return Inverse document frequency (logN)
   */
    /*public BigDecimal idf(final ByteArray term, final double logBase)
        throws DataProviderException {
      return
          BigDecimalMath.log(BigDecimalCache.get(numberOfDocuments())
              .divide(BigDecimalCache.get(df(term)), MATH_CONTEXT)
              .add(BigDecimal.ONE, MATH_CONTEXT))
              .divide(BigDecimalMath.log(BigDecimalCache.get(logBase)),
                  MATH_CONTEXT);
    }*/

  /**
   * Get the document frequency of a term.
   *
   * @param term Term to lookup.
   * @return Document frequency of the given term
   * @throws DataProviderException Forwarded from lower-level
   */
  public Integer df(final ByteArray term)
      throws DataProviderException {
    if (this.conf.cacheDf) {
      Integer result = this.c_df.get(term);
      if (result == null) {
        result = this.dataProv.getDocumentFrequency(term);
        this.c_df.put(term, result);
      }
      return result;
    } else {
      return this.dataProv.getDocumentFrequency(term);
    }
  }

  /**
   * Get the document frequency of a term.
   *
   * @param term Term to lookup.
   * @return Document frequency of the given term
   * @throws DataProviderException Forwarded from lower-level
   */
  public BigDecimal relDf(final ByteArray term)
      throws DataProviderException {
    return BigDecimal.valueOf((long) df(term)).divide(
        this.docCount, MATH_CONTEXT);
  }

  /**
   * Get a document data model from the {@link IndexDataProvider}.
   *
   * @param documentId Id of the document whose model to get
   * @return Document-model for the given document id
   * @throws DataProviderException Forwarded from lower-level
   */
  public DocumentModel docData(final int documentId)
      throws DataProviderException {
    if (this.conf.cacheDocModels) {
      DocumentModel d = this.c_docModel.get(documentId);
      if (d == null) {
        d = this.dataProv.getDocumentModel(documentId);
        this.c_docModel.put(documentId, d);
      }
      return d;
    } else {
      return this.dataProv.getDocumentModel(documentId);
    }
  }

  /**
   * Calculate the Okapi BM25 derivation of the inverse document frequency
   * (IDF) using a logarithmic base value of 10.
   *
   * @param term Term to calculate
   * @return Inverse document frequency BM25 (logN)
   */
    /*public BigDecimal idfBM25(final ByteArray term)
        throws DataProviderException {
      return idfBM25(term, 10d);
    }*/

  /**
   * Calculate the Okapi BM25 derivation of the inverse document frequency
   * (IDF) using a custom logarithmic base value.
   *
   * @param term Term to calculate
   * @param logBase Logarithmic base
   * @return Inverse document frequency BM25 (logN)
   */
    /*public BigDecimal idfBM25(final ByteArray term, final double logBase)
        throws DataProviderException {
      final int docFreq = df(term);

      return
          BigDecimalMath.log(
              BigDecimalCache.get(numberOfDocuments() - docFreq + 0.5)
                  .divide(BigDecimalCache.get(docFreq + 0.5), MATH_CONTEXT)
          ).divide(BigDecimalMath.log(BigDecimalCache.get(logBase)),
              MATH_CONTEXT);
    }*/
}
