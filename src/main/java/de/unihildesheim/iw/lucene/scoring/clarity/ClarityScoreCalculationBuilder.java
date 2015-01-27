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

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.lucene.scoring.ScoringBuilder.ScoringBuilderBase;
import de.unihildesheim.iw.util.Configuration;

/**
 * Base builder interface for clarity calculation implementations.
 *
 * @author Jens Bertram
 */
public abstract class ClarityScoreCalculationBuilder<S extends
    ClarityScoreCalculation, C extends Configuration>
    extends ScoringBuilderBase<ClarityScoreCalculationBuilder, C>
    implements Buildable<ClarityScoreCalculation> {

  /**
   * Configuration object for the implementing score.
   */
  private C conf;
  /**
   * Flag indicating, if pre-calculation of scoring values may be done by
   * implementation.
   */
  private boolean preCalculate;

  ClarityScoreCalculationBuilder(final String newIdentifier) {
    super(newIdentifier);
  }

  @Override
  public ClarityScoreCalculationBuilder configuration(final C configuration) {
    this.conf = configuration;
    return this;
  }

  public C getConfiguration() {
    return this.conf;
  }

  @Override
  public ClarityScoreCalculationBuilder getThis() {
    return this;
  }

  @Override
  public abstract S build()
      throws BuildableException;

  /**
   * Turns on pre-calculation of any values needed to perform the scoring.
   *
   * @return Self reference
   */
  public ClarityScoreCalculationBuilder preCalculate() {
    this.preCalculate = true;
    return this;
  }

  /**
   * Get the flag indicating, if the instance may pre-calculate values needed
   * for scoring.
   *
   * @return True, if pre-calculation may happen
   */
  public boolean shouldPrecalculate() {
    return this.preCalculate;
  }
}
