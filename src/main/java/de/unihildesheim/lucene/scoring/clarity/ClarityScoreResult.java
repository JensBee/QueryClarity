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
 * Wrapper class enclosing the results of a clarity sscore calculation.
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class ClarityScoreResult {

  /**
   * Calculated clarity score.
   */
  private double score = 0d;

  /**
   * Used implementation of {@link ClarityScoreCalculation}.
   */
  private Class<? extends ClarityScoreCalculation> type;

  /**
   * Create a new calculation result of the given type.
   *
   * @param cscType Class implementing {@link ClarityScoreCalculation}
   */
  ClarityScoreResult(final Class<? extends ClarityScoreCalculation> cscType) {
    this.type = cscType;
  }

  /**
   * Create a new calculation result of the given type with the given value.
   *
   * @param cscType Class implementing {@link ClarityScoreCalculation}
   * @param clarityScore Clarity score calculated by the given calculation type
   */
  ClarityScoreResult(final Class<? extends ClarityScoreCalculation> cscType,
          final double clarityScore) {
    this.type = cscType;
    setScore(clarityScore);
  }

  public final void clear() {
    // reset score
    setScore(0d);
  }

  /**
   * Get the calculated clarity score.
   *
   * @return Calculated clarity score
   */
  public final double getScore() {
    return this.score;
  }

  /**
   * Set the calculated clarity score.
   *
   * @param clarityScore Calculated clarity score
   */
  public final void setScore(final double clarityScore) {
    this.score = clarityScore;
  }

  /**
   * Get the type of calculation that created this result.
   *
   * @return Class implementing {@link ClarityScoreCalculation} that created
   * this result instance
   */
  public final Class<? extends ClarityScoreCalculation> getType() {
    return type;
  }

  /**
   * Set the type of calculation that created this result. If the new
   * <tt>class</tt> is not the same as the previously set <tt>class</tt> and the
   * currently set type is not <tt>null</tt> then all previously stored results
   * will be reset. This is done by calling the
   * {@link ClarityScoreResult#clear()} method.
   *
   * @param cscType Class implementing {@link ClarityScoreCalculation}
   */
  public final void setType(
          final Class<? extends ClarityScoreCalculation> cscType) {
    if (this.type != null && !cscType.equals(this.type)) {
      clear();
    }
    this.type = cscType;
  }
}
