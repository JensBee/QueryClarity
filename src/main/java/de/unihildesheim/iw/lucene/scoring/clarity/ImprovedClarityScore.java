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

import de.unihildesheim.iw.Buildable.BuildException;
import de.unihildesheim.iw.Buildable.BuildableException;
import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.Closable;
import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.Tuple.Tuple2;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.scoring.ScoringResult.ScoringResultXml.Keys;
import de.unihildesheim.iw.lucene.scoring.data.DefaultFeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.DefaultVocabularyProvider;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.VocabularyProvider;
import de.unihildesheim.iw.lucene.scoring.data.VocabularyProvider.Filter;
import de.unihildesheim.iw.util.MathUtils.KlDivergence;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.processing.IteratorSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall.TargetFunc;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Improved Clarity Score implementation as described by Hauff, Murdock,
 * Baeza-Yates. <br> Reference <br> Hauff, Claudia, Vanessa Murdock, and Ricardo
 * Baeza-Yates. “Improved Query Difficulty Prediction for the Web.” In
 * Proceedings of the 17th ACM Conference on Information and Knowledge
 * Management, 439–448. CIKM ’08. New York, NY, USA: ACM, 2008.
 * doi:10.1145/1458082.1458142.
 *
 * @author Jens Bertram
 */
public final class ImprovedClarityScore
    implements ClarityScoreCalculation {

  /**
   * Prefix to use to store calculated term-data values in cache and access
   * properties stored in the {@link de.unihildesheim.iw.lucene.index
   * .IndexDataProvider}.
   */
  static final String IDENTIFIER = "ICS";
  /**
   * Logger instance for this class.
   */
  static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      ImprovedClarityScore.class);
  /**
   * Default math context for model calculations.
   */
  private static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf().getString(
          DefaultKeys.MATH_CONTEXT.toString()));
  /**
   * {@link IndexDataProvider} to use.
   */
  // accessed by unit test
  @SuppressWarnings("PackageVisibleField")
  final IndexDataProvider dataProv;
  /**
   * Reader to access the Lucene index.
   */
  private final IndexReader idxReader;
  /**
   * Lucene query analyzer.
   */
  private final Analyzer analyzer;
  /**
   * Provider for feedback vocabulary.
   */
  private final VocabularyProvider vocProvider;
  /**
   * Provider for feedback documents.
   */
  private final FeedbackProvider fbProvider;
  /**
   * Configuration object used for all parameters of the calculation.
   */
  private final ImprovedClarityScoreConfiguration conf;
  /**
   * List of query terms provided.
   */
  // accessed from inner classes
  @SuppressWarnings("PackageVisibleField")
  Collection<ByteArray> queryTerms;
  /**
   * Object containing methods for model calculations.
   */
  private Model model;

  /**
   * Class wrapping all methods needed for calculation needed models. Also holds
   * results of the calculations.
   */
  private class Model
      implements Closable {
    /**
     * List of query terms issued.
     */
    private final Collection<ByteArray> queryTerms;
    /**
     * List of feedback documents to use.
     */
    private final Collection<Integer> feedbackDocs;
    /**
     * Stores the static part of the query model calculation for each document.
     */
    private final Map<Integer, BigDecimal> staticQueryModelParts;
    /**
     * Stores the static part of the smoothed model calculation for each
     * document.
     */
    private final Map<Integer, BigDecimal> staticSmoothingParts;
    /**
     * Document model calculation parameters: [0] Smoothing (mu), [1] Beta, [2]
     * Lambda, [3] 1 - Beta, [4] 1 - Lambda
     */
    private final BigDecimal[] dmParams;
    /**
     * Caches values of document and query models. Those need to be normalized
     * before calculating the score.
     */
    @SuppressWarnings("ProtectedField")
    protected Map<Long, Tuple2<BigDecimal, BigDecimal>> dataSets;
    /**
     * Counter for entries in {@link #dataSets}. Gets used as map key.
     */
    private final AtomicLong dataSetCounter;

    /**
     * Initialize the model calculation object.
     *
     * @param qt Query terms. Query terms not found in the collection (TF=0)
     * will be skipped.
     * @param fb Feedback documents
     * @param dmSmoothing Document model: Smoothing parameter value
     * @param dmBeta Document model: Beta parameter value
     * @param dmLambda Document model: Lambda parameter value
     * @throws DataProviderException Forwarded from lower-level
     */
    private Model(final Collection<ByteArray> qt,
        final Collection<Integer> fb,
        final double dmSmoothing, final double dmBeta, final double dmLambda)
        throws DataProviderException {
      LOG.debug("Create runtime cache.");

      this.dmParams = new BigDecimal[]{
          BigDecimal.valueOf(dmSmoothing),
          BigDecimal.valueOf(dmBeta),
          BigDecimal.valueOf(dmLambda),
          null, null // added afterwards
      };
      this.dmParams[3] = BigDecimal.ONE.subtract(
          this.dmParams[1], MATH_CONTEXT);
      this.dmParams[4] = BigDecimal.ONE.subtract(
          this.dmParams[2], MATH_CONTEXT);

      // add query terms, skip those not in index
      this.queryTerms = new ArrayList<>(qt.size());
      for (final ByteArray queryTerm : qt) {
        if (ImprovedClarityScore.this.dataProv.metrics().tf(queryTerm) > 0L) {
          this.queryTerms.add(queryTerm);
        }
      }

      // add feedback documents
      this.feedbackDocs = new ArrayList<>(fb.size());
      this.feedbackDocs.addAll(fb);

      // initialize other properties
      this.staticQueryModelParts = new ConcurrentHashMap<>(
          (int) (ImprovedClarityScore.this.conf.getMaxFeedbackDocumentsCount()
              * 1.5));
      this.staticSmoothingParts = new ConcurrentHashMap<>(
          (int) (ImprovedClarityScore.this.conf.getMaxFeedbackDocumentsCount()
              * 1.5));
      this.dataSets = new ConcurrentHashMap<>(2000);
      this.dataSetCounter = new AtomicLong(0L);
    }

    /**
     * Collection model.
     *
     * @param term Term to calculate the collection model value for
     * @return Collection model value
     * @throws DataProviderException Forwarded from lower-level
     */
    BigDecimal collection(final ByteArray term)
        throws DataProviderException {
      return ImprovedClarityScore.this.dataProv.metrics().relTf(term);
    }

    /**
     * Document model.
     *
     * @param docModel Document data model
     * @param term Term to calculate the document model value for
     * @return Document model value
     * @throws DataProviderException Forwarded from lower-level
     */
    BigDecimal document(final DocumentModel docModel, final ByteArray term)
        throws DataProviderException {
      // collection model of current term
      final BigDecimal cModel = collection(term);
      // try get calculated static smoothing part
      BigDecimal sSmooth = this.staticSmoothingParts.get(docModel.id);

      // calculate static smoothing part
      if (sSmooth == null) {
        sSmooth = BigDecimal.valueOf(docModel.tf())
            .add(this.dmParams[0]
                .multiply(BigDecimal.valueOf(docModel.termCount()),
                    MATH_CONTEXT), MATH_CONTEXT);
        this.staticSmoothingParts.put(docModel.id, sSmooth);
      }

      // smoothed document-term model
      final BigDecimal smoothing = BigDecimal.valueOf(docModel.tf(term))
          .add(this.dmParams[0].multiply(cModel), MATH_CONTEXT)
          .divide(sSmooth, MATH_CONTEXT);

      // final model calculation
      return this.dmParams[2]
          .multiply(this.dmParams[1].multiply(smoothing, MATH_CONTEXT)
              .add(this.dmParams[3].multiply(cModel,
                  MATH_CONTEXT), MATH_CONTEXT), MATH_CONTEXT)
          .add(this.dmParams[4].multiply(cModel,
              MATH_CONTEXT), MATH_CONTEXT);
    }

    /**
     * Query model for a single document.
     *
     * @param docModel Document data model
     * @param term Term to calculate the query model value for
     * @return Query model value
     * @throws DataProviderException Forwarded from lower-level
     */
    BigDecimal query(final DocumentModel docModel, final ByteArray term)
        throws DataProviderException {
      final BigDecimal result = document(docModel, term);

      BigDecimal staticPart = this.staticQueryModelParts.get(docModel.id);
      if (staticPart == null) {
        staticPart = BigDecimal.ONE;
        for (final ByteArray queryTerm : this.queryTerms) {
          staticPart = staticPart.multiply(
              document(docModel, queryTerm), MATH_CONTEXT);
        }
        this.staticQueryModelParts.put(docModel.id, staticPart);
      }

      return result.multiply(staticPart, MATH_CONTEXT);
    }

    /**
     * Query model for all feedback documents.
     *
     * @param term Term to calculate the query model value for
     * @return Query model value for all feedback documents
     * @throws DataProviderException Forwarded from lower-level
     */
    BigDecimal query(final ByteArray term)
        throws DataProviderException {
      BigDecimal result = BigDecimal.ZERO;

      for (final Integer docId : this.feedbackDocs) {
        result = result.add(query(
            ImprovedClarityScore.this.dataProv.metrics()
                .docData(docId), term), MATH_CONTEXT);
      }
      return result;
    }

    @Override
    public void close() {
      LOG.debug("Close runtime cache.");
    }
  }

  /**
   * Create a new instance using a builder.
   *
   * @param builder Builder to use for constructing the instance
   * @throws BuildableException Thrown, if building the cache instance failed
   */
  private ImprovedClarityScore(final Builder builder)
      throws BuildableException {
    Objects.requireNonNull(builder, "Builder was null.");

    // set configuration
    this.dataProv = builder.getIndexDataProvider();
    this.idxReader = builder.getIndexReader();
    this.conf = builder.getConfiguration();

    // check config
    try {
      if (this.conf.getMinFeedbackDocumentsCount() >
          this.dataProv.getDocumentCount()) {
        throw new IllegalStateException(
            "Required minimum number of feedback documents ("
                + this.conf.getMinFeedbackDocumentsCount() + ") is larger "
                + "or equal compared to the total amount of indexed documents "
                + "(" + this.dataProv.getDocumentCount()
                + "). Unable to provide feedback."
        );
      }
    } catch (final DataProviderException e) {
      throw new BuildException("Error while initializing.", e);
    }
    this.conf.debugDump();

    this.analyzer = builder.getAnalyzer();

    if (builder.getVocabularyProvider() != null) {
      this.vocProvider = builder.getVocabularyProvider();
    } else {
      this.vocProvider = new DefaultVocabularyProvider();
    }

    if (builder.getFeedbackProvider() != null) {
      this.fbProvider = builder.getFeedbackProvider();
    } else {
      this.fbProvider = new DefaultFeedbackProvider();
    }
  }


  /**
   * Close this instance and release any resources.
   */
  @Override
  public void close() {
    // NOP
  }

  /**
   * Calculates the improved clarity score for a given query.
   *
   * @param query Query to calculate for
   * @return Clarity score result object
   */
  @Override
  public Result calculateClarity(final String query)
      throws ClarityScoreCalculationException, DataProviderException {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(query, "Query was null."))) {
      throw new IllegalArgumentException("Query was empty.");
    }

    // result object
    final Result result = new Result(this.getClass());

    // get a normalized unique list of query terms
    // skips stopwords and removes unknown terms
    this.queryTerms = QueryUtils.tokenizeQuery(query,
        this.analyzer, this.dataProv.metrics());

    // check query term extraction result
    if (this.queryTerms.isEmpty()) {
      result.setEmpty("No query terms.");
      return result;
    }

    // save base data to result object
    result.setConf(this.conf);
    result.setQueryTerms(this.queryTerms);

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    // set of feedback documents to use for calculation.
    final Set<Integer> feedbackDocIds;
    try {
      feedbackDocIds = this.fbProvider
          .indexReader(this.idxReader)
          .analyzer(this.analyzer)
          .query(query)
          .fields(this.dataProv.getDocumentFields())
          .amount(
              this.conf.getMinFeedbackDocumentsCount(),
              this.conf.getMaxFeedbackDocumentsCount())
          .get();
    } catch (final Exception e) {
      final String msg = "Caught exception while getting feedback documents.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    if (feedbackDocIds.isEmpty()) {
      result.setEmpty("No feedback documents.");
      return result;
    } else if (feedbackDocIds.size() < this.conf
        .getMinFeedbackDocumentsCount()) {
      result.setEmpty("Not enough feedback documents. " +
          this.conf.getMinFeedbackDocumentsCount() +
          " requested, " + feedbackDocIds.size() + " retrieved.");
      return result;
    }
    result.setFeedbackDocIds(feedbackDocIds);

    // get document frequency threshold - allowed terms must be in bounds
    final BigDecimal minFreq = BigDecimal.valueOf(this.conf
        .getMinFeedbackTermSelectionThreshold());
    final BigDecimal maxFreq = BigDecimal.valueOf(this.conf
        .getMaxFeedbackTermSelectionThreshold());
    LOG.debug("Feedback term Document frequency threshold: {}%-{}%",
        minFreq.doubleValue() * 100d, maxFreq.doubleValue() * 100d);

    // do the final calculation for all feedback terms
    if (LOG.isTraceEnabled()) {
      LOG.trace("Using {} feedback documents for calculation. {}",
          feedbackDocIds.size(), feedbackDocIds);
    } else {
      LOG.debug("Using {} feedback documents for calculation.",
          feedbackDocIds.size());
    }

    this.model = new Model(this.queryTerms, feedbackDocIds,
        this.conf.getDocumentModelSmoothingParameter(), this.conf
        .getDocumentModelParamBeta(), this.conf.getDocumentModelParamLambda());

    LOG.info("Requesting feedback vocabulary.");
    final Iterator<ByteArray> fbTermsIt = this.vocProvider
        .indexDataProvider(this.dataProv)
        .documentIds(feedbackDocIds)
        .filter(new Filter() {

          @SuppressWarnings("ReturnOfNull")
          @Override
          public ByteArray filter(final ByteArray term)
              throws DataProviderException {
            final BigDecimal relDf = ImprovedClarityScore.this
                .dataProv.metrics().relDf(term);
            if (relDf.compareTo(minFreq) >= 0
                && relDf.compareTo(maxFreq) <= 0) {
              return term;
            }
            return null;
          }
        })
        .get();

    LOG.info("Calculating score values.");
    try {
      new Processing().setSourceAndTarget(
          new TargetFuncCall<>(
              new IteratorSource<>(fbTermsIt),
              new ScoreCalculatorTarget()
          )
      ).process();
    } catch (final ProcessingException e) {
      final String msg = "Caught exception while calculating score.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    LOG.info("Calculating final score.");
    result.setScore(
        KlDivergence.calc(
            this.model.dataSets.values(),
            KlDivergence.sumValues(this.model.dataSets.values())
        ).doubleValue());

    timeMeasure.stop();

    LOG.debug("Calculating improved clarity score for query {} "
            + "with {} document models took {}. {}",
        query, feedbackDocIds.size(),
        timeMeasure.getTimeString(), result.getScore()
    );

    this.model.close();
    return result;
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  /**
   * Extended result object containing additional meta information about what
   * values were actually used for calculation.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Result
      extends ClarityScoreResult {
    /**
     * Configuration that was used.
     */
    private ImprovedClarityScoreConfiguration conf;
    /**
     * Ids of feedback documents used for calculation.
     */
    private Set<Integer> feedbackDocIds;

    /**
     * Creates an object wrapping the result with meta information.
     *
     * @param cscType Type of the calculation class
     */
    public Result(final Class<? extends ClarityScoreCalculation> cscType) {
      super(cscType);
      this.feedbackDocIds = Collections.emptySet();
    }

    /**
     * Set the list of feedback documents used.
     *
     * @param fbDocIds List of feedback documents
     */
    void setFeedbackDocIds(final Set<Integer> fbDocIds) {
      this.feedbackDocIds = Collections.unmodifiableSet(
          Objects.requireNonNull(fbDocIds, "Feedback documents were null."));
    }

    /**
     * Set the configuration that was used.
     *
     * @param newConf Configuration used
     */
    void setConf(final ImprovedClarityScoreConfiguration newConf) {
      this.conf = Objects.requireNonNull(newConf, "Configuration was null.");
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
     * Configuration prefix.
     */
    private static final String CONF_PREFIX = IDENTIFIER + "-result";

    /**
     * Get the collection of feedback documents used for calculation.
     *
     * @return Feedback documents used for calculation
     */
    public Set<Integer> getFeedbackDocuments() {
      return Collections.unmodifiableSet(this.feedbackDocIds);
    }

    @Override
    public ScoringResultXml getXml() {
      final ScoringResultXml xml = new ScoringResultXml();

      getXml(xml);
      // number of feedback documents
      xml.getItems().put(
          Keys.FEEDBACK_DOCUMENTS.toString(),
          Integer.toString(this.feedbackDocIds.size()));

      // feedback documents
      if (GlobalConfiguration.conf()
          .getAndAddBoolean(CONF_PREFIX + "ListFeedbackDocuments",
              Boolean.TRUE)) {
        final List<Tuple.Tuple2<String, String>> fbDocsList = new ArrayList<>
            (this.feedbackDocIds.size());
        for (final Integer docId : this.feedbackDocIds) {
          fbDocsList.add(Tuple.tuple2(
              Keys.FEEDBACK_DOCUMENT_KEY.toString(),
              docId.toString()));
        }
        xml.getLists().put(Keys.FEEDBACK_DOCUMENTS.toString(), fbDocsList);
      }

      return xml;
    }


  }

  /**
   * Builder to create a new {@link ImprovedClarityScore} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends ClarityScoreCalculationBuilder<ImprovedClarityScore,
      ImprovedClarityScoreConfiguration> {
    /**
     * Initializes the builder.
     */
    public Builder() {
      super(IDENTIFIER);
    }

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    public ImprovedClarityScore build()
        throws BuildableException {
      validate();
      return new ImprovedClarityScore(this);
    }

    @Override
    public void validate()
        throws ConfigurationException {
      new Validator(this, new Feature[]{
          Feature.CONFIGURATION,
          Feature.ANALYZER,
          Feature.CACHE,
          Feature.DATA_PATH,
          Feature.DATA_PROVIDER,
          Feature.INDEX_READER
      });
    }
  }

  /**
   * {@link Processing} {@link Target} calculating a portion of the final
   * clarity score. The current term is passed in from a {@link Source}.
   */
  private final class ScoreCalculatorTarget
      extends TargetFunc<ByteArray> {

    /**
     * Calculate the score portion for a given term using already calculated
     * query models.
     */
    @Override
    public void call(final ByteArray term)
        throws DataProviderException {
      if (term != null) {
        ImprovedClarityScore.this.model.dataSets.put(
            ImprovedClarityScore.this.model.dataSetCounter.incrementAndGet(),
            Tuple.tuple2(ImprovedClarityScore.this.model.query(term),
                ImprovedClarityScore.this.dataProv.metrics().relTf(term)));
      }
    }
  }
}
