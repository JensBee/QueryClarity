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

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for {@link DefaultClarityScore}.
 *
 * @author Jens Bertram
 */
public final class DefaultClarityScoreConfiguration
    extends Configuration {

  /**
   * Default initial configuration.
   */
  private static final Map<String, String> DEFAULTS;

  /**
   * Create a new configuration object with a default configuration set.
   */
  public DefaultClarityScoreConfiguration() {
    super(DEFAULTS);
  }

  /**
   * Get the language model weight parameter.
   *
   * @return Language model weight parameter value
   */
  public Double getLangModelWeight() {
    return getDouble(Keys.LANG_MODEL_WEIGHT.name());
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
  public Integer getFeedbackDocCount() {
    return getInteger(Keys.FB_DOC_COUNT.name());
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

  // initialize defaults map
  static {
    DEFAULTS = new HashMap<>(Keys.values().length);
    /**
     * Number of feedback documents to use.
     * <br>
     * Cronen-Townsend et al. recommend 500 documents.
     */
    DEFAULTS.put(Keys.FB_DOC_COUNT.name(), "500");
    /**
     * Default multiplier value for relative term frequency inside documents.
     * <br>
     * Cronen-Townsend, Steve, Yun Zhou, and W. Bruce Croft used 0.6 for this
     * parameter.
     */
    DEFAULTS.put(Keys.LANG_MODEL_WEIGHT.name(), "0.6");
  }
}
