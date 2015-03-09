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

package de.unihildesheim.iw.lucene.search;

import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;

/**
 * {@link DefaultSimilarity} instance changed to be used with {@link
 * FilteredDirectoryReader}.
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class FDRDefaultSimilarity
    extends DefaultSimilarity {
  /**
   * Same as {@link TFIDFSimilarity#idfExplain(CollectionStatistics,
   * TermStatistics)} but uses {@link CollectionStatistics#docCount()} instead
   * of {@link CollectionStatistics#maxDoc()}.
   *
   * @param collectionStats collection-level statistics
   * @param termStats term-level statistics for the term
   * @return an Explain object that includes both an idf score factor and an
   * explanation for the term.
   * @see TFIDFSimilarity#idfExplain(CollectionStatistics, TermStatistics)
   */
  @Override
  public Explanation idfExplain(
      final CollectionStatistics collectionStats,
      final TermStatistics termStats) {
    final long df = termStats.docFreq();
    final long max = collectionStats.docCount();
    final float idf = idf(df, max);
    return new Explanation(idf, "idf(docFreq=" + df + ", maxDocs=" + max + ')');
  }

  /**
   * Same as {@link TFIDFSimilarity#idfExplain(CollectionStatistics,
   * TermStatistics[])}, but uses {@link CollectionStatistics#docCount()}
   * instead of {@link CollectionStatistics#maxDoc()}.
   *
   * @param collectionStats collection-level statistics
   * @param termStats term-level statistics for the terms in the phrase
   * @return an Explain object that includes both an idf score factor for the
   * phrase and an explanation for each term.
   * @see TFIDFSimilarity#idfExplain(CollectionStatistics, TermStatistics[])
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Override
  public Explanation idfExplain(
      final CollectionStatistics collectionStats,
      final TermStatistics termStats[]) {
    final long max = collectionStats.docCount();
    float idf = 0.0f;
    final Explanation exp = new Explanation();
    exp.setDescription("idf(), sum of:");
    for (final TermStatistics stat : termStats) {
      final long df = stat.docFreq();
      final float termIdf = idf(df, max);
      exp.addDetail(new Explanation(termIdf,
          "idf(docFreq=" + df + ", maxDocs=" + max + ')'));
      idf += termIdf;
    }
    exp.setValue(idf);
    return exp;
  }
}
