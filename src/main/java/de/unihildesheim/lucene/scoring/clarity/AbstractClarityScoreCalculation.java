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

import de.unihildesheim.lucene.query.QueryTools;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public abstract class AbstractClarityScoreCalculation implements
        ClarityScoreCalculation {

  public abstract double calculateClarity(final String[] queryTerms);

  /**
   *
   * @param reader
   * @param query
   * @return
   * @throws IOException
   */
  public double calculateClarity(final IndexReader reader, final Query query)
          throws IOException {
    return calculateClarity(QueryTools.getQueryTerms(reader, query));
  }
}
