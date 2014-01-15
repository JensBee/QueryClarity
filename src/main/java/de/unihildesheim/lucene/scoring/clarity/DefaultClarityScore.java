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
package de.unihildesheim.lucene.scoring.clarity;

import de.unihildesheim.lucene.index.IndexDataProvider;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.slf4j.LoggerFactory;

/**
 * Default Clarity Score implementation as defined by Cronen-Townsend, Steve,
 * Yun Zhou, and W. Bruce Croft.
 *
 * Reference:
 *
 * “Predicting Query Performance.” In Proceedings of the 25th Annual
 * International ACM SIGIR Conference on Research and Development in Information
 * Retrieval, 299–306. SIGIR ’02. New York, NY, USA: ACM, 2002.
 * doi:10.1145/564376.564429.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class DefaultClarityScore extends AbstractClarityScoreCalculation {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          DefaultClarityScore.class);

  /**
   * Provider for statistical index related informations.
   */
  private IndexDataProvider dataProv;

  /**
   * Index reader used by this instance.
   */
  private IndexReader reader;

  /**
   * Default constructor using the {@link IndexDataProvider} for statistical
   * index data.
   *
   * @param indexReader {@link IndexReader} to use by this instance
   * @param dataProvider Provider for statistical index data
   */
  public DefaultClarityScore(final IndexReader indexReader,
          final IndexDataProvider dataProvider) {
    super();
    this.reader = indexReader;
    this.dataProv = dataProvider;
  }

  @Override
  public final double calculateClarity(final Query query) {
    double score;
    try {
      score = calculateClarity(this.reader, query);
    } catch (IOException ex) {
      score = 0d;
      LOG.error("Error calculating clarity score.", ex);
    }
    return score;
  }

  @Override
  public final double calculateClarity(final String[] queryTerms) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  /**
   * Get the provider for statistical index related informations used by this
   * instance.
   *
   * @return Data provider used by this instance
   */
  public final IndexDataProvider getDataProv() {
    return dataProv;
  }

  /**
   * Set the provider for statistical index related informations used by this
   * instance.
   *
   * @param dataProv Data provider to use by this instance
   */
  public final void setDataProv(final IndexDataProvider dataProv) {
    this.dataProv = dataProv;
  }

  /**
   * Get the {@link IndexReader} used by this instance.
   *
   * @return {@link IndexReader} used by this instance
   */
  public final IndexReader getReader() {
    return reader;
  }

  /**
   * Set the {@link IndexReader} used by this instance.
   *
   * @param reader {@link IndexReader} to use by this instance
   */
  public final void setReader(final IndexReader reader) {
    this.reader = reader;
  }
}
