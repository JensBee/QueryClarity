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
 * Configuration for {@link ImprovedClarityScore}.
 */
public final class ImprovedClarityScoreConfiguration
    extends Configuration {

  /**
   * Lambda value for calculating document models.
   * <br>
   * Hauff, Murdock & Baeza-Yates used the value 1 for their tests.
   */
  private static final double DEFAULT_DOCMODEL_PARAM_LAMBDA = 1d;
  /**
   * Beta value for calculating document models. This is related to the
   * lambda value used in the original Clarity Score.
   * <br>
   * Cronen-Townsend, Steve, Yun Zhou, and W. Bruce Croft used 0.6 for this
   * parameter.
   */
  private static final double DEFAULT_DOCMODEL_PARAM_BETA = 0.6;
  /**
   * Smoothing parameter (mu) for document model calculation.
   * <br>
   * Hauff, Murdock & Baeza-Yates used the values 100, 500, 1000, 1500,
   * 2000, 2500, 3000 and 5000 for their tests.
   */
  private static final double DEFAULT_DOCMODEL_SMOOTHING = 100d;
  /**
   * Minimum number of feedback documents to retrieve. If the amount of
   * feedback documents retrieved is lower than this value, the query will
   * be simplified to retrieve more results.
   * <br>
   * Hauff, Murdock & Baeza-Yates: every document must match all query
   * terms, if this is not the case, the query should be relaxed to get at
   * least one match.
   */
  private static final int DEFAULT_FB_DOCS_MIN = 1;
  /**
   * Maximum number of feedback documents to use. A positive Integer or
   * {@code -1} to set this to all matching documents.
   * <br>
   * Hauff, Murdock & Baeza-Yates evaluated with 10, 50, 100. 250, 500, 700,
   * 1000 documents.
   */
  private static final int DEFAULT_FB_DOCS_MAX = 500;
  /**
   * Threshold to select terms from feedback documents. A term from a
   * feedback document must occur in min n% of the documents
   * in the index. If it's not the case it will be ignored.
   * <br>
   * A value of {@code 0} sets the minimum number of documents that must
   * match to {@code 1}.
   * <br>
   * Hauff, Murdock & Baeza-Yates evaluated n with 1% (0.01), 10% (0.1),
   * 100% (1).
   */
  @SuppressWarnings("ConstantNamingConvention")
  private static final double DEFAULT_TERM_SELECTION_THRESHOLD_MIN = 0d;
  /**
   * Threshold to select terms from feedback documents. A term from a
   * feedback document must occur in max n% of the documents
   * in the index. If it's not the case it will be ignored.
   * <br>
   * Hauff, Murdock & Baeza-Yates evaluated n with 1% (0.01), 10% (0.1),
   * any (disables threshold) (1).
   */
  @SuppressWarnings("ConstantNamingConvention")
  private static final double DEFAULT_TERM_SELECTION_THRESHOLD_MAX = 0.1;

  /**
   * Get the maximum number of feedback documents that will be used.
   *
   * @return Maximum number of feedback documents to get
   */
  public int getMaxFeedbackDocumentsCount() {
    return getInteger(Keys.FB_DOCS_MAX.name(), DEFAULT_FB_DOCS_MAX);
  }

  /**
   * Set the maximum number of feedback documents that will be used.
   *
   * @param count Maximum number of feedback documents to get
   */
  public void setMaxFeedbackDocumentsCount(final int count) {
    add(Keys.FB_DOCS_MAX.name(), count);
  }

  /**
   * Get the minimum number of feedback documents that will be used.
   *
   * @return Minimum number of feedback documents to get
   */
  public int getMinFeedbackDocumentsCount() {
    return getInteger(Keys.FB_DOCS_MIN.name(), DEFAULT_FB_DOCS_MIN);
  }

  /**
   * Set the minimum number of feedback documents that will be used.
   *
   * @param count Minimum number of feedback documents to get
   */
  public void setMinFeedbackDocumentsCount(final int count) {
    add(Keys.FB_DOCS_MIN.name(), count);
  }

  /**
   * Get the smoothing parameter (mu) used for document model calculation.
   *
   * @return Smoothing parameter value
   */
  public double getDocumentModelSmoothingParameter() {
    return getDouble(
        Keys.DOCMODEL_SMOOTHING.name(), DEFAULT_DOCMODEL_SMOOTHING);
  }

  /**
   * Set the smoothing parameter (mu) used for document model calculation.
   *
   * @param param Smoothing parameter value
   */
  public void setDocumentModelSmoothingParameter(final double param) {
    add(Keys.DOCMODEL_SMOOTHING.name(), param);
  }

  /**
   * Get the minimum threshold used to select terms from feedback documents.
   *
   * @return Threshold value
   */
  public double getMinFeedbackTermSelectionThreshold() {
    return getDouble(Keys.TERM_SELECTION_THRESHOLD_MIN.name(),
        DEFAULT_TERM_SELECTION_THRESHOLD_MIN);
  }

  /**
   * Get the maximum threshold used to select terms from feedback documents.
   *
   * @return Threshold value
   */
  public double getMaxFeedbackTermSelectionThreshold() {
    return getDouble(Keys.TERM_SELECTION_THRESHOLD_MAX.name(),
        DEFAULT_TERM_SELECTION_THRESHOLD_MAX);
  }

  /**
   * Set the threshold used to select terms from feedback documents. If a term
   * occurs in lower than threshold% documents in index, it will be ignored.
   *
   * @param thresholdMin Minimum threshold % expressed as double value
   * @param thresholdMax Maximum threshold % expressed as double value
   */
  public void setFeedbackTermSelectionThreshold(final double thresholdMin,
      final double thresholdMax) {
    if (thresholdMin > thresholdMax) {
      throw new IllegalArgumentException("Minimum value is larger than " +
          "maximum.");
    }
    add(Keys.TERM_SELECTION_THRESHOLD_MIN.name(), thresholdMin);
    add(Keys.TERM_SELECTION_THRESHOLD_MAX.name(), thresholdMax);
  }

  /**
   * Get the lambda parameter value used for document model calculation.
   *
   * @return Lambda parameter value used for document model calculation
   */
  public double getDocumentModelParamLambda() {
    return getDouble(
        Keys.DOCMODEL_PARAM_LAMBDA.name(), DEFAULT_DOCMODEL_PARAM_LAMBDA);
  }

  /**
   * Set the lambda parameter value used for document model calculation.
   *
   * @param lambda Lambda parameter value used for document model calculation.
   * This value should be greater than 0 and lower or equal than/to 1.
   */
  public void setDocumentModelParamLambda(final double lambda) {
    add(Keys.DOCMODEL_PARAM_LAMBDA.name(), lambda);
  }

  /**
   * Set the beta parameter value used for document model calculation.
   *
   * @return Beta parameter value used for document model calculation.
   */
  public double getDocumentModelParamBeta() {
    return getDouble(
        Keys.DOCMODEL_PARAM_BETA.name(), DEFAULT_DOCMODEL_PARAM_BETA);
  }

  /**
   * Set the beta parameter value used for document model calculation.
   *
   * @param beta Beta parameter value used for document model calculation. This
   * value should be greater than 0 and lower than 1.
   */
  public void setDocumentModelParamBeta(final double beta) {
    add(Keys.DOCMODEL_PARAM_BETA.name(), beta);
  }

  /**
   * Keys to identify properties in the configuration.
   */
  private static enum Keys {

    /**
     * Document-model calculation alpha parameter.
     */
    DOCMODEL_PARAM_LAMBDA,
    /**
     * Document-model calculation beta parameter.
     */
    DOCMODEL_PARAM_BETA,
    /**
     * Smoothing parameter for document model calculation.
     */
    DOCMODEL_SMOOTHING,
    /**
     * Minimum number of feedback documents to retrieve.
     */
    FB_DOCS_MIN,
    /**
     * Maximum number of feedback documents to retrieve.
     */
    FB_DOCS_MAX,
    /**
     * Document-frequency threshold to pick terms from feedback documents.
     * Minimum value (lower range).
     */
    TERM_SELECTION_THRESHOLD_MIN,
    /**
     * Document-frequency threshold to pick terms from feedback documents.
     * Maximum value (upper range).
     */
    TERM_SELECTION_THRESHOLD_MAX
  }
}
