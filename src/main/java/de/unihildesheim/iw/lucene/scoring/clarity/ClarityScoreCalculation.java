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
package de.unihildesheim.iw.lucene.scoring.clarity;

/**
 * Generic interface for various clarity score calculation implementations.
 *
 * @author Jens Bertram
 */
public interface ClarityScoreCalculation {

  /**
   * Calculate the clarity score based on the given query terms.
   *
   * @param query Query used for term extraction
   * @return Calculated clarity score for the given terms, or <tt>null</tt> on
   * errors.
   * @throws Exception May be thrown by implementing class
   */
  ClarityScoreResult calculateClarity(final String query)
      throws ClarityScoreCalculationException;

  /**
   * Basic class wrapping errors occurring while calculating the Clarity Score.
   * May be extended by implementing classes to provide finer grained error
   * tracing.
   */
  public class ClarityScoreCalculationException
      extends Exception {
    public ClarityScoreCalculationException(final Exception ex) {
      super("Failed to calculate clarity score.", ex);
    }

    public ClarityScoreCalculationException(final String msg, final Exception
        ex) {
      super(msg, ex);
    }

    public ClarityScoreCalculationException(final String msg) {
      super(msg);
    }
  }
}
