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
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.MathUtils;

/**
 * Collection related metrics.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class CollectionMetrics {

  /**
   * Private constructor for utility class.
   */
  private CollectionMetrics() {
    // empty utility constructor
  }

  /**
   * Get the number of documents in the index.
   *
   * @return Number of documents in index
   */
  public static Long numberOfDocuments() {
    return Environment.getDataProvider().getDocumentCount();
  }

  /**
   * Get the document frequency of a term.
   *
   * @param term Term to lookup.
   * @return Document frequency of the given term
   */
  public static Integer df(final BytesWrap term) {
    return Environment.getDataProvider().getDocumentFrequency(term);
  }

  /**
   * Get the overall raw term frequency of all terms in the index.
   *
   * @return Collection term frequency
   */
  public static Long tf() {
    return Environment.getDataProvider().getTermFrequency();
  }

  /**
   * Get the raw frequency of a given term in the collection.
   *
   * @param term Term to lookup
   * @return Collection frequency of the given term
   */
  public static Long tf(final BytesWrap term) {
    return Environment.getDataProvider().getTermFrequency(term);
  }

  /**
   * Get the relative frequency of a term. The relative frequency is the
   * frequency <tt>tF</tt> of term <tt>t</tt> divided by the frequency
   * <tt>F</tt> of all terms.
   *
   * @param term Term to lookup
   * @return Relative collection frequency of the given term
   */
  public static Double relTf(final BytesWrap term) {
    return Environment.getDataProvider().getRelativeTermFrequency(term);
  }

  /**
   * Calculate the inverse document frequency (IDF) using a logarithmic base
   * of 10.
   *
   * @param term Term to calculate
   * @return Inverse document frequency (log10)
   */
  public static Double idf(final BytesWrap term) {
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
  public static Double idf(final BytesWrap term, final double logBase) {
    return MathUtils.logN(logBase, 1 + (numberOfDocuments() / df(term)));
  }

  /**
   * Calculate the Okapi BM25 derivation of the inverse document frequency
   * (IDF) using a logarithmic base value of 10.
   *
   * @param term Term to calculate
   * @return Inverse document frequency BM25 (logN)
   */
  public static Double idfBM25(final BytesWrap term) {
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
  public static Double idfBM25(final BytesWrap term, final double logBase) {
    final int docFreq = df(term);
    return MathUtils.logN(logBase, (numberOfDocuments() - docFreq + 0.5)
            / (docFreq + 0.5));
  }

  /**
   * Calculate the term frequency–inverse document frequency for a given term.
   * The raw term frequency and the simple IDF calculation with a logarithmic
   * base of 10 are used.
   *
   * @param term Term to calculate
   * @return Term frequency–inverse document frequency
   */
  public static double tfIdf(final BytesWrap term) {
    return tf(term) * idf(term);
  }
}
