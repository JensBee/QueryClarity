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
package de.unihildesheim.lucene.scoring.clarity.impl;

import de.unihildesheim.util.Configuration;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for {@link DefaultClarityScore}.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DefaultClarityScoreConfiguration extends Configuration {

  /**
   * Default initial configuration.
   */
  private static final Map<String, String> defaults;

  /**
   * Keys to identify properties in the configuration.
   */
  private static enum Keys {

    /**
     * Number of feedback documents to use.
     */
    fbDocCount,
    /**
     * Document model calculation weighting parameter.
     */
    langModelWeight
  }

  // initialize defaults map
  static {
    defaults = new HashMap<String, String>();
    /**
     * Number of feedback documents to use.
     * <p>
     * Cronen-Townsend et al. recommend 500 documents.
     */
    defaults.put(Keys.fbDocCount.name(), "500");
    /**
     * Default multiplier value for relative term frequency inside documents.
     * <p>
     * Cronen-Townsend, Steve, Yun Zhou, and W. Bruce Croft used 0.6 for this
     * parameter.
     */
    defaults.put(Keys.langModelWeight.name(), "0.6");
  }

  /**
   * Create a new configuration object with a default configuration set.
   */
  public DefaultClarityScoreConfiguration() {
    super(defaults);
  }

  /**
   * Get the language model weight parameter.
   *
   * @return Language model weight parameter value
   */
  public Double getLangModelWeight() {
    return getDouble(Keys.langModelWeight.name());
  }

  /**
   * Set the language model weight parameter.
   *
   * @param newWeight New language model weight parameter value
   */
  public void setLangModelWeight(final double newWeight) {
    add(Keys.langModelWeight.name(), newWeight);
  }

  /**
   * Get the number of feedback documents used.
   *
   * @return Number of feedback documents
   */
  public Integer getFeedbackDocCount() {
    return getInteger(Keys.fbDocCount.name());
  }

  /**
   * Set the number of feedback documents to use.
   *
   * @param newCount Number of feedback documents to use
   */
  public void setFeedbackDocCount(final int newCount) {
    add(Keys.fbDocCount.name(), newCount);
  }
}
