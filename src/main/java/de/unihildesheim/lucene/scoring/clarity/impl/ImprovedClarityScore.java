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

import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.Feedback;
import de.unihildesheim.lucene.metrics.CollectionMetrics;
import de.unihildesheim.lucene.metrics.DocumentMetrics;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.lucene.query.TermsQueryBuilder;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreCalculation;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.MathUtils;
import de.unihildesheim.util.RandomValue;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.slf4j.LoggerFactory;

/**
 * Improved Clarity Score implementation as described by Hauff, Murdock,
 * Baeza-Yates.
 * <p>
 * Reference
 * <p>
 * Hauff, Claudia, Vanessa Murdock, and Ricardo Baeza-Yates. “Improved Query
 * Difficulty Prediction for the Web.” In Proceedings of the 17th ACM
 * Conference on Information and Knowledge Management, 439–448. CIKM ’08. New
 * York, NY, USA: ACM, 2008. doi:10.1145/1458082.1458142.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class ImprovedClarityScore implements ClarityScoreCalculation {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          ImprovedClarityScore.class);
  /**
   * Prefix to use to store calculated term-data values in cache and access
   * properties stored in the {@link DataProvider}.
   */
  static final String PREFIX = "ICS";

  /**
   * Configuration object used for all parameters of the calculation.
   */
  private final ImprovedClarityScoreConfiguration conf;

  /**
   * Cache for calculated document model values.
   */
  private Map<String, Double> docModelCache;

  /**
   * Policy to use to simplify a query, if no document matches all terms in
   * the initial query.
   * <p>
   * If multiple terms match the same criteria a random one out of those will
   * be chosen.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum QuerySimplifyPolicy {

    /**
     * Removes the first term.
     */
    FIRST,
    /**
     * Removes the term with the highest document-frequency.
     */
    HIGHEST_DOCFREQ,
    /**
     * Removes the term with the highest index-frequency.
     */
    HIGHEST_TERMFREQ,
    /**
     * Removes the last term.
     */
    LAST,
    /**
     * Removes a randomly chosen term.
     */
    RANDOM;
  }

  /**
   * Create a new scoring instance with the default parameter set.
   */
  public ImprovedClarityScore() {
    this(new ImprovedClarityScoreConfiguration());
  }

  /**
   * Create a new scoring instance with the parameters set in the given
   * configuration.
   *
   * @param newConf Configuration
   */
  public ImprovedClarityScore(final ImprovedClarityScoreConfiguration newConf) {
    super();
    this.conf = newConf;
    Environment.getDataProvider().registerPrefix(PREFIX);
    this.docModelCache = new ConcurrentHashMap<>(this.conf.
            getMaxFeedbackDocumentsCount());
    parseConfig();
  }

  /**
   * Parse the configuration and do some simple pre-checks.
   */
  private void parseConfig() {
    if (this.conf.getMinFeedbackDocumentsCount() > CollectionMetrics.
            numberOfDocuments()) {
      throw new IllegalStateException(
              "Required minimum number of feedback documents ("
              + this.conf.getMinFeedbackDocumentsCount() + ") is larger "
              + "or equal compared to the total amount of indexed documents "
              + "(" + CollectionMetrics.numberOfDocuments()
              + "). Unable to provide feedback.");
    }
    this.conf.debugDump();
  }

  /**
   * Reduce the query by removing a term based on a specific policy.
   *
   * @param query Query to reduce
   * @param policy Policy to use for choosing which term to remove
   */
  private String simplifyQuery(final String query,
          final QuerySimplifyPolicy policy) throws IOException,
          UnsupportedEncodingException, ParseException {
    Collection<BytesWrap> qTerms = new ArrayList<>(QueryUtils.
            getAllQueryTerms(query));
    BytesWrap termToRemove = null;
    if (new HashSet<>(qTerms).size() == 1) {
      LOG.debug("Return empty string from one term query.");
      return "";
    }

    switch (policy) {
      case FIRST:
        termToRemove = ((List<BytesWrap>) qTerms).get(0);
        break;
      case HIGHEST_DOCFREQ:
        long docFreq = 0;
        qTerms = new HashSet<>(qTerms);
        for (BytesWrap term : qTerms) {
          final long tDocFreq = CollectionMetrics.df(term);
          if (tDocFreq > docFreq) {
            termToRemove = term.clone();
            docFreq = tDocFreq;
          } else if (tDocFreq == docFreq && RandomValue.getBoolean()) {
            termToRemove = term.clone();
          }
        }
        break;
      case HIGHEST_TERMFREQ:
        long collFreq = 0;
        qTerms = new HashSet<>(qTerms);
        for (BytesWrap term : qTerms) {
          final long tCollFreq = CollectionMetrics.tf(term);
          if (tCollFreq > collFreq) {
            termToRemove = term.clone();
            collFreq = tCollFreq;
          } else if (tCollFreq == collFreq && RandomValue.getBoolean()) {
            termToRemove = term.clone();
          }
        }
        break;
      case LAST:
        termToRemove = ((List<BytesWrap>) qTerms).get(qTerms.size() - 1);
        break;
      case RANDOM:
        final int idx = RandomValue.getInteger(0, qTerms.size() - 1);
        termToRemove = ((List<BytesWrap>) qTerms).get(idx);
        break;
    }

    while (qTerms.contains(termToRemove)) {
      qTerms.remove(termToRemove);
    }

    final StringBuilder sb = new StringBuilder(100);
    for (BytesWrap qTerm : qTerms) {
      sb.append(qTerm.toString()).append(' ');
    }

    LOG.debug("Remove term={} policy={} oldQ={} newQ={}", termToRemove.
            toString(), policy, query, sb.toString().trim());
    return sb.toString().trim();
  }

  /**
   * Calculate the document model for a given term. The document model is
   * calculated using Bayesian smoothing using Dirichlet priors.
   *
   * @param docId Document id
   * @param term Term to calculate the model for
   * @return Calculated document model given the term
   */
  private double calcDocumentModel(final int docId, final BytesWrap term) {
    Double model = this.docModelCache.get(docId + term.toString());

    if (model == null) {
      final double smoothing = this.conf.getDocumentModelSmoothingParameter();
      final double lambda = this.conf.getDocumentModelParamLambda();
      final double beta = this.conf.getDocumentModelParamBeta();

      final DocumentMetrics dm = new DocumentMetrics(docId);

      // total frequency of all terms in document
      final double totalFreq = dm.termFrequency().doubleValue();
      // term frequency given the document
      final double termFreq = dm.termFrequency(term).doubleValue();
      final double uniqueTerms = dm.termCount().doubleValue();
      // relative collection frequency of the term
      final double rCollFreq = CollectionMetrics.relTf(term);

      model = (termFreq + (smoothing * rCollFreq)) / (totalFreq + (smoothing
              * uniqueTerms));

      model = (lambda * ((beta * model) + ((1 - beta) * rCollFreq))) + ((1
              - lambda) * rCollFreq);

      this.docModelCache.put(docId + term.toString(), model);
    }

    return model;
  }

  /**
   * Calculate the product of all feedback documents given the query terms.
   *
   * @return Product of all document models given the query terms
   * @throws UnsupportedEncodingException Thrown, if a query term could not be
   * parsed
   */
  private double calcDocModelQueryTermsProduct(
          final Integer fbDocId, final Collection<BytesWrap> qTerms) throws
          UnsupportedEncodingException {
    double product = 1L;
    // iterate through all query terms
    for (BytesWrap qTerm : qTerms) {
      product *= calcDocumentModel(fbDocId, qTerm);
    }
    return product;
  }

  /**
   *
   * @param qTerms List of query terms
   * @param qTermIdx Index of the current query term to calculate
   * @param fbDocIds List of feedback document
   * @return Query model for the current term and set of feedback documents
   * @throws UnsupportedEncodingException Thrown, if a query term could not be
   * parsed
   */
  private double calcQueryModel(final BytesWrap fbTerm,
          final Collection<BytesWrap> qTerms,
          final Collection<Integer> fbDocIds) throws
          UnsupportedEncodingException {
    double model = 0d;

    for (Integer fbDocId : fbDocIds) {
      // document model for the given term pD(t)
      final double docModel = calcDocumentModel(fbDocId, fbTerm);
      // calculate the product of the document models for all query terms
      // given the current document
      final double docModelQtProduct = calcDocModelQueryTermsProduct(fbDocId,
              qTerms);
      model += docModel * docModelQtProduct;
    }
    return model;
  }

  @Override
  public Result calculateClarity(String query) throws
          ParseException {
    if (query == null || query.isEmpty()) {
      throw new IllegalArgumentException("Query was empty.");
    }

    // result object
    final Result result = new Result(this.getClass());
    // final clarity score
    double score = 0;
    // collection of feedback document ids
    Collection<Integer> feedbackDocIds;
    // save base data to result object
    result.queries.add(query);
    result.conf = this.conf;

    // run a query to get feedback
    TermsQueryBuilder qBuilder = new TermsQueryBuilder().setBoolOperator(
            QueryParser.Operator.AND);
    Query queryObj = qBuilder.buildUsingEnvironment(query);
    try {
      feedbackDocIds = new HashSet<>(this.conf.
              getMaxFeedbackDocumentsCount());
      feedbackDocIds.addAll(Feedback.get(queryObj, this.conf.
              getMaxFeedbackDocumentsCount()));

      // simplify query, if not enough feedback documents are available
      String simplifiedQuery = query;
      int docsToGet;
      while (feedbackDocIds.size() < this.conf.getMinFeedbackDocumentsCount()) {
        // set flag indicating we simplified the query
        result.wasQuerySimplified = true;
        LOG.info("Minimum number of feedback documents not reached "
                + "({}/{}). Simplifying query using {} policy.",
                feedbackDocIds.size(), this.conf.
                getMinFeedbackDocumentsCount(), this.conf.
                getQuerySimplifyingPolicy());

        simplifiedQuery = simplifyQuery(simplifiedQuery, this.conf.
                getQuerySimplifyingPolicy());

        if (simplifiedQuery.isEmpty()) {
          throw new IllegalStateException(
                  "No query terms left while trying "
                  + "to reach the minimum nmber of feedback documents.");
        }
        result.queries.add(simplifiedQuery);
        docsToGet = this.conf.getMaxFeedbackDocumentsCount()
                - feedbackDocIds.size();
        queryObj = qBuilder.buildUsingEnvironment(simplifiedQuery);
        feedbackDocIds.addAll(Feedback.get(queryObj, docsToGet));
      }

      // collect all unique terms from feedback documents
      @SuppressWarnings("CollectionWithoutInitialCapacity")
      final List<BytesWrap> fbTerms = new ArrayList<>(Environment.
              getDataProvider().getDocumentsTermSet(feedbackDocIds));
      // get document frequency threshold
      int minDf = (int) (CollectionMetrics.numberOfDocuments()
              * this.conf.getFeedbackTermSelectionThreshold());
      if (minDf <= 0) {
        LOG.debug("Document frequency threshold was {} setting to 1", minDf);
        minDf = 1;
      }
      LOG.debug("Document frequency threshold is {} = {}", minDf, this.conf.
              getFeedbackTermSelectionThreshold());
      LOG.debug("Initial term set size {}", fbTerms.size());
      final Iterator<BytesWrap> fbTermsIt = fbTerms.iterator();
      // remove all terms whose threshold is too low
      while (fbTermsIt.hasNext()) {
        final BytesWrap term = fbTermsIt.next();
        if (CollectionMetrics.df(term) < minDf) {
          fbTermsIt.remove();
        }
      }
      LOG.debug("Reduced term set size {}", fbTerms.size());

      // do the final calculation for all remaining feedback terms
      final Collection<BytesWrap> queryTerms = QueryUtils.getAllQueryTerms(
              query);
      for (int i = 0; i < fbTerms.size(); i++) {
        // query model for the current term
        final double queryModel = calcQueryModel(fbTerms.get(i), queryTerms,
                feedbackDocIds);
        score += queryModel * MathUtils.log2(queryModel / CollectionMetrics.
                relTf(fbTerms.get(i)).doubleValue());
      }

      LOG.debug("Using {} feedback documents.", feedbackDocIds.size());

      result.set(score);
      result.feedbackDocIds = feedbackDocIds;
      result.feedbackTerms = fbTerms;

      LOG.debug(
              "Calculation results: query={} docModels={} terms={} score={}.",
              query, feedbackDocIds.size(), fbTerms.size(), score);
    } catch (IOException ex) {
      LOG.error("Caught exception while retrieving feedback documents.", ex);
    }

    return result;
  }

  /**
   * Extended result object containing additional meta information about what
   * values were actually used for calculation.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Result extends ClarityScoreResult {

    /**
     * Configuration that was used.
     */
    private ImprovedClarityScoreConfiguration conf;
    /**
     * Ids of feedback documents used for calculation.
     */
    private Collection<Integer> feedbackDocIds;
    /**
     * Terms from feedback documents used for calculation.
     */
    private Collection<BytesWrap> feedbackTerms;
    /**
     * Flag indicating, if the query was simplified.
     */
    private boolean wasQuerySimplified = false;
    /**
     * List of queries issued to get feedback documents.
     */
    private List<String> queries;

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    public Result(Class<? extends ClarityScoreCalculation> cscType) {
      super(cscType);
      this.queries = new ArrayList<>();
      this.feedbackDocIds = Collections.emptyList();
      this.feedbackTerms = Collections.emptyList();
    }

    /**
     * Set the calculation result.
     *
     * @param score
     */
    private void set(final double score) {
      super.setScore(score);
    }

    /**
     * Get the configuration used for this calculation result.
     *
     * @return Configuration used for this calculation result
     */
    public ImprovedClarityScoreConfiguration getConfiguration() {
      return this.conf;
    }

    /**
     * Get the collection of feedback documents used for calculation.
     *
     * @return Feedback documents used for calculation
     */
    public Collection<Integer> getFeedbackDocuments() {
      return Collections.unmodifiableCollection(this.feedbackDocIds);
    }

    /**
     * Get the collection of feedback terms used for calculation.
     *
     * @return Feedback terms used for calculation
     */
    public Collection<BytesWrap> getFeedbackTerms() {
      return Collections.unmodifiableCollection(this.feedbackTerms);
    }

    /**
     * Get the flag indicating, if the query was simplified.
     *
     * @return True, if it was simplified
     */
    public boolean wasQuerySimplified() {
      return this.wasQuerySimplified;
    }

    /**
     * Get the queries issued to get feedback documents.
     *
     * @return List of queries issued
     */
    public List<String> getQueries() {
      return Collections.unmodifiableList(this.queries);
    }
  }
}
