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

import de.unihildesheim.iw.util.Configuration;

/**
 * Configuration for {@link DefaultClarityScore}.
 *
 * @author Jens Bertram
 */
public final class DefaultClarityScoreConfiguration
    extends Configuration {
  /**
   * Default multiplier value for relative term frequency inside documents.
   * Cronen-Townsend, Steve, Yun Zhou, and W. Bruce Croft used 0.6 for this
   * parameter.
   */
  private static final double DEFAULT_LANG_MODEL_WEIGHT = 0.6;
  /**
   * Number of feedback documents to use.
   * <br>
   * Cronen-Townsend et al. recommend 500 documents.
   */
  private static final int DEFAULT_FB_DOC_COUNT = 500;

  /**
   * Get the language model weight parameter.
   *
   * @return Language model weight parameter value
   */
  public Double getLangModelWeight() {
    return getDouble(Keys.LANG_MODEL_WEIGHT.name(), DEFAULT_LANG_MODEL_WEIGHT);
  }

  /**
   * Set the language model weight parameter.
   *
   * @param newWeight New language model weight parameter value
   */
  public void setLangModelWeight(final double newWeight) {
    add(Keys.LANG_MODEL_WEIGHT.name(), newWeight);
  }

  /**
   * Get the number of feedback documents used.
   *
   * @return Number of feedback documents
   */
  public int getFeedbackDocCount() {
    return getInteger(Keys.FB_DOC_COUNT.name(), DEFAULT_FB_DOC_COUNT);
  }

  /**
   * Set the number of feedback documents to use.
   *
   * @param newCount Number of feedback documents to use
   */
  public void setFeedbackDocCount(final int newCount) {
    add(Keys.FB_DOC_COUNT.name(), newCount);
  }

  /**
   * Keys to identify properties in the configuration.
   */
  private static enum Keys {

    /**
     * Number of feedback documents to use.
     */
    FB_DOC_COUNT,
    /**
     * Document-model calculation weighting parameter.
     */
    LANG_MODEL_WEIGHT
  }
}
