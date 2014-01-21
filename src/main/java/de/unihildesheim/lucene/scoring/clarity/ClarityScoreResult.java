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

/**
 * Wrapper class enclosing the results of a clarity score calculation.
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class ClarityScoreResult {

  /**
   * Calculated clarity score.
   */
  private final Double score;

  /**
   * Used implementation of {@link ClarityScoreCalculation}.
   */
  private final Class<? extends ClarityScoreCalculation> type;

  /**
   * Create a new calculation result of the given type with the given value.
   *
   * @param cscType Class implementing {@link ClarityScoreCalculation}
   * @param clarityScore Clarity score calculated by the given calculation type
   */
  ClarityScoreResult(final Class<? extends ClarityScoreCalculation> cscType,
          final double clarityScore) {
    this.type = cscType;
    this.score = clarityScore;
  }

  /**
   * Create a new calculation result of the given type with no result.
   *
   * @param cscType Class implementing {@link ClarityScoreCalculation}
   */
  ClarityScoreResult(final Class<? extends ClarityScoreCalculation> cscType) {
    this.type = cscType;
    this.score = null;
  }

  /**
   * Get the calculated clarity score.
   *
   * @return Calculated clarity score
   */
  public double getScore() {
    return this.score;
  }

  /**
   * Get the type of calculation that created this result.
   *
   * @return Class implementing {@link ClarityScoreCalculation} that created
   * this result instance
   */
  public Class<? extends ClarityScoreCalculation> getType() {
    return type;
  }
}
