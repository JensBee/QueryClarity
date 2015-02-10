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

package de.unihildesheim.iw.lucene.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;

import java.util.Set;

/**
 * @author Jens Bertram
 */
public abstract class RelaxableQuery {

  RelaxableQuery(final Analyzer analyzer,
      final String qString,
      final Set<String> fields) {
    // Empty constructor to provide a common interface. Parameters are unused
    // here.
  }

  /**
   * Relaxes (simplifies) the query, e.g. to get more results.
   *
   * @return True, if query was relaxed, false otherwise
   * @throws ParseException Thrown, if relaxed query could not be parsed
   */
  @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
  public abstract boolean relax();

  /**
   * Get the Query object.
   *
   * @return Query object
   */
  public abstract Query getQueryObj();
}
