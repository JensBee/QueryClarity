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
package de.unihildesheim.lucene.scoring;

import de.unihildesheim.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.lucene.scoring.clarity.impl.DefaultClarityScore;
import de.unihildesheim.lucene.scoring.clarity.impl.ImprovedClarityScore;
import de.unihildesheim.lucene.scoring.clarity.impl.SimplifiedClarityScore;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class Scoring {

  /**
   * Different types of clarity score calculators.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum ClarityScore {

    /**
     * Default Clarity Score.
     */
    DEFAULT,
    /**
     * Improved Clarity Score.
     */
    IMPROVED,
    /**
     * Simplified Clarity Score.
     */
    SIMPLIFIED
  }

  /**
   * Private utility class constructor.
   */
  private Scoring() {
    // empty
  }

  /**
   * Create a new Clarity Score calculation instance of a specific type.
   * @param csType Type of clarity score
   * @return New instance usable for calculating the specified score type
   */
  public static ClarityScoreCalculation newInstance(final ClarityScore csType) {
    switch (csType) {
      case DEFAULT:
        return new DefaultClarityScore();
      case IMPROVED:
        return new ImprovedClarityScore();
      case SIMPLIFIED:
        return new SimplifiedClarityScore();
    }
    throw new IllegalArgumentException(
            "Unknown or not supported type specified.");
  }
}
