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
import java.util.Objects;

/**
 * Configuration for {@link ImprovedClarityScore}.
 */
public final class ImprovedClarityScoreConfiguration
    extends Configuration {

  /**
   * Default initial configuration.
   */
  private static final Map<String, String> defaults;

  /**
   * Keys to identify properties in the configuration.
   */
  private static enum Keys {

    /**
     * Document-model calculation alpha parameter.
     */
    docModelParamLambda,
    /**
     * Document-model calculation beta parameter.
     */
    docModelParamBeta,
    /**
     * Smoothing parameter for document model calculation.
     */
    docModelSmoothing,
    /**
     * Minimum number of feedback documents to retrieve.
     */
    fbDocsMin,
    /**
     * Maximum number of feedback documents to retrieve.
     */
    fbDocsMax,
    /**
     * Policy to use for simplifying queries. See {@link
     * ImprovedClarityScore.QuerySimplifyPolicy}.
     */
    querySimplifyingPolicy,
    /**
     * Document-frequency threshold to pick terms from feedback documents.
     */
    termSelectionThreshold
  }

  // initialize defaults map
  static {
    defaults = new HashMap<>(Keys.values().length);
    /**
     * Lambda value for calculating document models.
     * <p>
     * Hauff, Murdock & Baeza-Yates used the value 1 for their tests.
     */
    defaults.put(Keys.docModelParamLambda.name(), "1");
    /**
     * Beta value for calculating document models. This is related to the
     * lambda value used in the original Clarity Score.
     * <p>
     * Cronen-Townsend, Steve, Yun Zhou, and W. Bruce Croft used 0.6 for this
     * parameter.
     */
    defaults.put(Keys.docModelParamBeta.name(), "0.6");
    /**
     * Smoothing parameter (mu) for document model calculation.
     * <p>
     * Hauff, Murdock & Baeza-Yates used the values 100, 500, 1000, 1500,
     * 2000, 2500, 3000 and 5000 for their tests.
     */
    defaults.put(Keys.docModelSmoothing.name(), "100");
    /**
     * Minimum number of feedback documents to retrieve. If the amount of
     * feedback documents retrieved is lower than this value, the query will
     * be simplified to retrieve more results.
     * <p>
     * Hauff, Murdock & Baeza-Yates used a value of 10 for theirs tests.
     */
    defaults.put(Keys.fbDocsMin.name(), "10");
    /**
     * Maximum number of feedback documents to use.
     * <p>
     * Hauff, Murdock & Baeza-Yates used a value of 1000 for theirs tests.
     */
    defaults.put(Keys.fbDocsMax.name(), "1000");
    /**
     * Default policy to use for simplifying the query, if the number of
     * feedback documents is lower than the required minimum.
     */
    defaults.put(Keys.querySimplifyingPolicy.name(),
        ImprovedClarityScore.QuerySimplifyPolicy.HIGHEST_DOCFREQ.name());
    /**
     * Threshold to select terms from feedback documents. A term from a
     * feedback document must occurs in equal or more than n% of the documents
     * in the index. If it's not the case it will be ignored.
     * <p>
     * Hauff, Murdock & Baeza-Yates evaluated n with 1% (0.01), 10% (0.1),
     * 100% (1).
     */
    defaults.put(Keys.termSelectionThreshold.name(), "0.1");
  }

  /**
   * Create a new configuration object with a default configuration set.
   */
  public ImprovedClarityScoreConfiguration() {
    super(defaults);
  }

  /**
   * Get the maximum number of feedback documents that will be used.
   *
   * @return Maximum number of feedback documents to get
   */
  public Integer getMaxFeedbackDocumentsCount() {
    return getInteger(Keys.fbDocsMax.name());
  }

  /**
   * Set the maximum number of feedback documents that will be used.
   *
   * @param count Maximum number of feedback documents to get
   */
  public void setMaxFeedbackDocumentsCount(final int count) {
    add(Keys.fbDocsMax.name(), count);
  }

  /**
   * Get the minimum number of feedback documents that will be used.
   *
   * @return Minimum number of feedback documents to get
   */
  public Integer getMinFeedbackDocumentsCount() {
    return getInteger(Keys.fbDocsMin.name());
  }

  /**
   * Set the minimum number of feedback documents that will be used.
   *
   * @param count Minimum number of feedback documents to get
   */
  public void setMinFeedbackDocumentsCount(final int count) {
    add(Keys.fbDocsMin.name(), count);
  }

  /**
   * Get the policy that will be used to simplify a query, if the minimum number
   * of feedback documents could not be reached with the original query.
   *
   * @return Query simplifying policy to use
   */
  public ImprovedClarityScore.QuerySimplifyPolicy getQuerySimplifyingPolicy() {
    final String policy = getString(Keys.querySimplifyingPolicy.name());
    return ImprovedClarityScore.QuerySimplifyPolicy.valueOf(policy);
  }

  /**
   * Set the policy that will be used to simplify a query, if the minimum number
   * of feedback documents could not be reached with the original query.
   *
   * @param policy Query simplifying policy to use
   */
  public void setQuerySimplifyingPolicy(
      final ImprovedClarityScore.QuerySimplifyPolicy policy) {
    Objects.requireNonNull(policy);
    add(Keys.querySimplifyingPolicy.name(), policy.name());
  }

  /**
   * Get the smoothing parameter (mu) used for document model calculation.
   *
   * @return Smoothing parameter value
   */
  public Double getDocumentModelSmoothingParameter() {
    return getDouble(Keys.docModelSmoothing.name());
  }

  /**
   * Set the smoothing parameter (mu) used for document model calculation.
   *
   * @param param Smoothing parameter value
   */
  public void setDocumentModelSmoothingParameter(final double param) {
    add(Keys.docModelSmoothing.name(), param);
  }

  /**
   * Get the threshold used to select terms from feedback documents.
   *
   * @return Threshold value
   */
  public Double getFeedbackTermSelectionThreshold() {
    return getDouble(Keys.termSelectionThreshold.name());
  }

  /**
   * Set the threshold used to select terms from feedback documents. If a term
   * occurs in lower than threshold% documents in index, it will be ignored.
   *
   * @param threshold Threshold % expressed as double value
   */
  public void setFeedbackTermSelectionThreshold(final double threshold) {
    add(Keys.termSelectionThreshold.name(), threshold);
  }

  /**
   * Get the lambda parameter value used for document model calculation.
   *
   * @return Lambda parameter value used for document model calculation
   */
  public Double getDocumentModelParamLambda() {
    return getDouble(Keys.docModelParamLambda.name());
  }

  /**
   * Set the lambda parameter value used for document model calculation.
   *
   * @param lambda Lambda parameter value used for document model calculation.
   * This value should be greater than 0 and lower or equal than/to 1.
   */
  public void setDocumentModelParamLambda(final double lambda) {
    add(Keys.docModelParamLambda.name(), lambda);
  }

  /**
   * Set the beta parameter value used for document model calculation.
   *
   * @return Beta parameter value used for document model calculation.
   */
  public Double getDocumentModelParamBeta() {
    return getDouble(Keys.docModelParamBeta.name());
  }

  /**
   * Set the beta parameter value used for document model calculation.
   *
   * @param beta Beta parameter value used for document model calculation. This
   * value should be greater than 0 and lower than 1.
   */
  public void setDocumentModelParamBeta(final double beta) {
    add(Keys.docModelParamBeta.name(), beta);
  }

  /**
   * Compacts the configration into a more accessible Object.
   *
   * @return Compact configuration object
   */
  public Conf compile() {
    return new Conf();
  }

  /**
   * Compact configuration Object holding all information from the parent
   * configuration Object.
   */
  public class Conf {
    public final Double beta;
    public final Double lambda;
    public final Double smoothing;
    public final Double threshold;
    public final Integer fbMax;
    public final Integer fbMin;
    public final ImprovedClarityScore.QuerySimplifyPolicy policy;

    protected Conf() {
      this.beta = getDocumentModelParamBeta();
      this.lambda = getDocumentModelParamLambda();
      this.smoothing = getDocumentModelSmoothingParameter();
      this.threshold = getFeedbackTermSelectionThreshold();
      this.fbMax = getMaxFeedbackDocumentsCount();
      this.fbMin = getMinFeedbackDocumentsCount();
      this.policy = getQuerySimplifyingPolicy();
    }

    public void debugDump() {
      ImprovedClarityScoreConfiguration.this.debugDump();
    }
  }
}
