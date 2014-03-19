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
import java.util.Iterator;

/**
 * Document related metrics.
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DocumentMetrics {

  /**
   * Private constructor for utility class.
   */
  private DocumentMetrics() {
    // empty utility constructor
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
   * @param documentId Id of target document
   * @param term Term whose frequency to get
   * @return Frequency of the given term in the given document
   */
  public static Long termFrequency(final int documentId,
          final BytesWrap term) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null");
    }
    final Long freq = Environment.getDataProvider().getDocumentModel(
            documentId).termFreqMap.get(term);
    if (freq == null) {
      return 0L;
    }
    return freq;
  }

  /**
   * Get the number of unique terms in document.
   *
   * @param documentId Id of target document
   * @return Number of unique terms in document
   */
  public static Long uniqueTermCount(final int documentId) {
    final Integer count = Environment.getDataProvider().getDocumentModel(
            documentId).termFreqMap.size();
    // check for case where are more than Integer.MAX_VALUE entries
    if (count == Integer.MAX_VALUE) {
      Long manualCount = 0L;
      Iterator<BytesWrap> termsIt = Environment.getDataProvider().
              getDocumentModel(documentId).termFreqMap.keySet().iterator();
      while (termsIt.hasNext()) {
        manualCount++;
        termsIt.next();
      }
      return manualCount;
    }
    return count.longValue();
  }
}
