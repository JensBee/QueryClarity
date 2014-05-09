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
import de.unihildesheim.iw.SoftHashMap;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.util.MathUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * High level wrapper for {@link IndexDataProvider} to provide collection and
 * document related metrics.
 * <p/>
 * Beware that {@link DocumentModel}s get cached (per {@link Metrics} instance).
 * If you change parameters that affect the contents of the {@link
 * DocumentModel}s (e.g. stopwords, document fields) you should create a new
 * {@link Metrics} instance with a fresh cache.
 *
 * @author Jens Bertram
 */
public final class Metrics {

  private static final Map<IndexDataProvider, Metrics> CACHE = new
      WeakHashMap<>();

  private final IndexDataProvider dataProv;
  public final CollectionMetrics collection;
  private final Map<Integer, DocumentModel> docModelCache = new SoftHashMap<>();

  private Metrics(final IndexDataProvider dataProvider) {
    this.dataProv = dataProvider;
    this.collection = new CollectionMetrics();
  }

  public static Metrics getInstance(final IndexDataProvider dataProv) {
    Metrics m;
    m = CACHE.get(dataProv);
    if (m == null) {
      m = new Metrics(dataProv);
      CACHE.put(dataProv, m);
    }
    return m;
  }

  public DocumentMetrics document(final DocumentModel docModel) {
    if (docModel == null) {
      throw new IllegalArgumentException("Document model was null.");
    }
    return new DocumentMetrics(docModel);
  }

  public DocumentMetrics document(final int docId) {
    return document(getDocumentModel(docId));
  }

  /**
   * Get a document model from the {@link IndexDataProvider}.
   *
   * @param documentId Id of the document whose model to get
   * @return Document-model for the given document id
   */
  public DocumentModel getDocumentModel(final int documentId) {
    DocumentModel d;
    d = docModelCache.get(documentId);
    if (d == null) {
      d = dataProv.getDocumentModel(documentId);
      docModelCache.put(documentId, d);
    }
    return d;
  }

  /**
   * High level wrapper for {@link IndexDataProvider} to provide collection
   * related metrics.
   *
   * @author Jens Bertram
   */
  public final class CollectionMetrics {
    /**
     * Get the number of unique terms in the index.
     *
     * @return Number of unique terms in index
     */
    public Long numberOfUniqueTerms() {
      return dataProv.getUniqueTermsCount();
    }

    /**
     * Get the number of documents in the index.
     *
     * @return Number of documents in index
     */
    public Long numberOfDocuments() {
      return dataProv.getDocumentCount();
    }

    /**
     * Get the document frequency of a term.
     *
     * @param term Term to lookup.
     * @return Document frequency of the given term
     */
    public Integer df(final ByteArray term) {
      return dataProv.getDocumentFrequency(term);
    }

    /**
     * Get the raw frequency of a given term in the collection.
     *
     * @param term Term to lookup
     * @return Collection frequency of the given term
     */
    public Long tf(final ByteArray term) {
      return dataProv.getTermFrequency(term);
    }

    /**
     * Get the overall raw term frequency of all terms in the index.
     *
     * @return Collection term frequency
     */
    public Long tf() {
      return dataProv.getTermFrequency();
    }

    /**
     * Get the relative frequency of a term. The relative frequency is the
     * frequency <tt>tF</tt> of term <tt>t</tt> divided by the frequency
     * <tt>F</tt> of all terms.
     *
     * @param term Term to lookup
     * @return Relative collection frequency of the given term
     */
    public Double relTf(final ByteArray term) {
      return dataProv.getRelativeTermFrequency(term);
    }

    /**
     * Calculate the inverse document frequency (IDF) using a logarithmic base
     * of 10.
     *
     * @param term Term to calculate
     * @return Inverse document frequency (log10)
     */
    public Double idf(final ByteArray term) {
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
    public Double idf(final ByteArray term, final double logBase) {
      return MathUtils.logN(logBase, 1 + (numberOfDocuments() / df(term)));
    }

    /**
     * Calculate the Okapi BM25 derivation of the inverse document frequency
     * (IDF) using a logarithmic base value of 10.
     *
     * @param term Term to calculate
     * @return Inverse document frequency BM25 (logN)
     */
    public Double idfBM25(final ByteArray term) {
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
    public Double idfBM25(final ByteArray term, final double logBase) {
      final int docFreq = df(term);
      return MathUtils.logN(logBase, (numberOfDocuments() - docFreq + 0.5)
          / (docFreq + 0.5));
    }

    /**
     * Iterate over all terms in the index providing each term and it's index
     * frequency.
     *
     * @return Iterator providing <tt>term, index</tt> frequency value pairs
     */
    public Iterator<Tuple.Tuple2<ByteArray, Long>> termFrequencyIterator() {
      return new Iterator<Tuple.Tuple2<ByteArray, Long>>() {
        private final Iterator<ByteArray> idxTerms =
            dataProv.getTermsIterator();

        @Override
        public boolean hasNext() {
          return idxTerms.hasNext();
        }

        @Override
        public Tuple.Tuple2<ByteArray, Long> next() {
          final ByteArray term = idxTerms.next();
          return Tuple.tuple2(term, dataProv.getTermFrequency(term));
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("Not supported yet.");
        }
      };
    }
  }

  /**
   * High level wrapper for {@link IndexDataProvider} to provide document
   * related metrics.
   *
   * @author Jens Bertram
   */
  public static final class DocumentMetrics {

    private final DocumentModel docModel;

    public DocumentMetrics(final DocumentModel documentModel) {
      this.docModel = documentModel;
    }

    /**
     * Calculate the Within-document Frequency for the given term.
     *
     * @param term Term whose frequency to get
     * @return Frequency value
     */
    public double wdf(final ByteArray term) {
      return MathUtils.log2(tf(term).doubleValue() + 1) /
          MathUtils.log2(tf().doubleValue());
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
     * Get the frequency of the given term in the specific document.
     *
     * @param term Term whose frequency to get
     * @return Frequency of the given term in the given document
     */
    public Long tf(final ByteArray term) {
      if (term == null) {
        throw new IllegalArgumentException("Term was null.");
      }
      final Long freq = this.docModel.termFreqMap.get(term);
      if (freq == null) {
        return 0L;
      }
      return freq;
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
     * Get the relative term frequency for a term in the document. Calculated by
     * dividing the frequency of the given term by the frequency of all terms in
     * the document.
     *
     * @param term Term to lookup
     * @return Relative frequency. Zero if term is not in document.
     */
    public Double relTf(final ByteArray term) {
      Long tf = this.docModel.tf(term);
      if (tf == 0) {
        return 0.0;
      }
      return tf.doubleValue() / Long.valueOf(this.docModel.termFrequency)
          .doubleValue();
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
     * Get the relative term frequency for a term in the document. Calculated
     * using Bayesian smoothing using Dirichlet priors.
     *
     * @param term Term to lookup
     * @param smoothing Smoothing parameter
     * @return Smoothed relative term frequency
     * @see #smoothedRelativeTermFrequency(ByteArray, double)
     */
    public double smoothedRelativeTermFrequency(final ByteArray term,
        final double smoothing) {
      final double termFreq = this.docModel.tf(term).doubleValue();
      final double relCollFreq = relTf(term);
      return ((termFreq + smoothing) * relCollFreq) / (termFreq + (Integer.
          valueOf(this.docModel.termFreqMap.size()).doubleValue() * smoothing));
    }
  }
}
