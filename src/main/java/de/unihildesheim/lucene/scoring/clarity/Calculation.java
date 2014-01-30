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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class Calculation {

  /**
   * Shared index reader instance.
   */
  private IndexReader indexReader;

  /**
   * Data provider for cacheable index statistics.
   */
  private final IndexDataProvider dataProv;

  public Calculation(final IndexDataProvider dataProvider,
          final IndexReader reader) {
    this.dataProv = dataProvider;
    this.indexReader = reader;
  }

  public final IndexReader getIndexReader() {
    return this.indexReader;
  }

  public final void setIndexReader(final IndexReader reader) {
    this.indexReader = reader;
  }

  /**
   *
   * @param query User query to parse
   * @throws IOException Thrown if index could not be read
   */
  public final void calculateClarity(final Query query) throws IOException {
    DefaultClarityScore dcs = new DefaultClarityScore(indexReader, dataProv);
    ClarityScoreResult csr = dcs.calculateClarity(query);
  }
}
