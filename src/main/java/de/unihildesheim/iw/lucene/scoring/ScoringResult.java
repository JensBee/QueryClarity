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
package de.unihildesheim.iw.lucene.scoring;

/**
 * Wrapper to store scoring results.
 *
 * @author Jens Bertram
 */
public abstract class ScoringResult {

  /**
   * Calculated result score.
   */
  private Double score = 0d;

  /**
   * Get the calculated score.
   *
   * @return Calculated score
   */
  public final double getScore() {
    return this.score;
  }

  /**
   * Set the value of the calculated score.
   *
   * @param newScore new score value
   */
  @SuppressWarnings("checkstyle:methodname")
  protected final void _setScore(final double newScore) {
    this.score = newScore;
  }

  /**
   * Get the type of calculation that created this result.
   *
   * @return Class creating a scoring result
   */
  public abstract Class getType();
}
