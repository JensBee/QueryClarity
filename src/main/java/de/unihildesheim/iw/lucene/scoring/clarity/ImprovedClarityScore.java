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

import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.query.QueryUtils;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation
    .ScoreTuple.TupleType;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreResult
    .EmptyReason;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.VocabularyProvider;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import de.unihildesheim.iw.lucene.util.DocIdSetUtils;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import de.unihildesheim.iw.util.Buildable.BuildableException;
import de.unihildesheim.iw.util.GlobalConfiguration;
import de.unihildesheim.iw.util.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.util.MathUtils;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.Tuple;
import de.unihildesheim.iw.util.Tuple.Tuple2;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanQuery.TooManyClauses;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

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
    extends AbstractClarityScoreCalculation {
  private static final String MSG_NO_THRESHOLDED_FEEDBACK_TERMS =
      "No feedback terms available after reducing by threshold.";
  /**
   * Prefix to use to store calculated term-data values in cache and access
   * properties stored in the {@link de.unihildesheim.iw.lucene.index
   * .IndexDataProvider}.
   */
  @SuppressWarnings("WeakerAccess")
  public static final String IDENTIFIER = "ICS";
  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      ImprovedClarityScore.class);
  /**
   * Default math context for model calculations.
   */
  @SuppressWarnings("WeakerAccess")
  static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf()
          .getString(DefaultKeys.MATH_CONTEXT.toString(),
              GlobalConfiguration.DEFAULT_MATH_CONTEXT));
  /**
   * If true, low precision math is used for doing calculations.
   */
  private static final boolean MATH_LOW_PRECISION = GlobalConfiguration.conf()
      .getBoolean(DefaultKeys.MATH_LOW_PRECISION.toString(), false);
  /**
   * {@link IndexDataProvider} to use.
   */
  private final IndexDataProvider dataProv;
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
   * Abstract model class. Shared methods for low/high precision model
   * calculations.
   */
  private abstract static class AbstractModel {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(AbstractModel.class);
    /**
     * List of query terms issued.
     */
    final BytesRefArray queryTerms;
    /**
     * List of feedback documents to use.
     */
    final int[] feedbackDocs;
    /**
     * Stores the feedback document models.
     */
    final Map<Integer, DocumentModel> docModels;
    /**
     * IndexDataProvider instance.
     */
    final IndexDataProvider dataProv;

    /**
     * Initialize the abstract model with a set of query terms and feedback
     * documents.
     *
     * @param dataProv DataProvider
     * @param qt Query terms
     * @param fb Feedback documents
     * @throws IOException Thrown on low-level I/O-errors
     */
    AbstractModel(final IndexDataProvider dataProv,
        final BytesRefArray qt, final DocIdSet fb)
        throws IOException {
      LOG.debug("Create runtime cache.");
      this.dataProv = dataProv;
      // add query terms, skip those not in index
      this.queryTerms = new BytesRefArray(Counter.newCounter(false));
      StreamUtils.stream(qt)
          .filter(queryTerm -> this.dataProv.getTermFrequency(queryTerm) > 0L)
          .forEach(this.queryTerms::append);

      // init store for cached document models
      this.docModels = new ConcurrentHashMap<>((int) (
          (double) DocIdSetUtils.cardinality(fb) * 1.8));

      // init feedback documents list
      LOG.info("Caching document models");
      this.feedbackDocs = StreamUtils.stream(fb)
          .peek(docId -> this.docModels.put(docId,
              this.dataProv.getDocumentModel(docId)))
          .toArray();
    }
  }

  /**
   * Class wrapping all methods needed for calculation needed models. Also holds
   * results of the calculations.
   */
  private static final class ModelHighPrecision
      extends AbstractModel {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(ModelHighPrecision.class);
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
     * Initialize the model calculation object.
     *
     * @param dataProv DataProvider instance from parent class
     * @param qt Query terms. Query terms not found in the collection (TF=0)
     * will be skipped.
     * @param fb Feedback documents
     * @param dmSmoothing Document model: Smoothing parameter value
     * @param dmBeta Document model: Beta parameter value
     * @param dmLambda Document model: Lambda parameter value
     * @throws IOException Thrown on low-level I/O-errors
     */
    ModelHighPrecision(final IndexDataProvider dataProv,
        final BytesRefArray qt, final DocIdSet fb,
        final double dmSmoothing, final double dmBeta, final double dmLambda)
        throws IOException {
      super(dataProv, qt, fb);

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

      // initialize other properties
      this.staticQueryModelParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.length * 1.8));
      this.staticSmoothingParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.length * 1.8));

      LOG.info("Pre-calculating static query model and smoothing values");
      for (final Integer docId : this.feedbackDocs) {
        final DocumentModel docModel = this.dataProv.getDocumentModel(docId);

        // smoothing value
        final BigDecimal sSmooth = BigDecimal.valueOf(docModel.tf())
            .add(this.dmParams[0]
                .multiply(BigDecimal.valueOf((long) docModel.termCount()),
                    MATH_CONTEXT), MATH_CONTEXT);
        this.staticSmoothingParts.put(docId, sSmooth);

        // static query model part (needs smoothing values)
        final BigDecimal staticPart = StreamUtils.stream(this.queryTerms)
            .map(br -> document(docModel, br))
            .reduce(BigDecimal.ONE, (r, c) -> r.multiply(c, MATH_CONTEXT));
        this.staticQueryModelParts.put(docId, staticPart);
      }
    }

    /**
     * Document model.
     *
     * @param docModel Document data model
     * @param term Term to calculate the document model value for
     * @return Document model value
     */
    BigDecimal document(
        final DocumentModel docModel, final BytesRef term) {
      // collection model of current term
      final BigDecimal cModel = BigDecimal.valueOf(
          this.dataProv.getRelativeTermFrequency(term));

      // smoothed document-term model
      final BigDecimal smoothingPart = this.staticSmoothingParts
          .get(docModel.id);
      final BigDecimal smoothing;
      if (smoothingPart.compareTo(BigDecimal.ZERO) == 0) {
        smoothing = BigDecimal.ZERO;
      } else {
        smoothing = BigDecimal.valueOf(docModel.tf(term))
            .add(this.dmParams[0].multiply(cModel), MATH_CONTEXT)
            .divide(this.staticSmoothingParts.get(docModel.id), MATH_CONTEXT);
      }

      // final model calculation
      return this.dmParams[2]
          .multiply(this.dmParams[1].multiply(smoothing, MATH_CONTEXT)
              .add(this.dmParams[3].multiply(cModel,
                  MATH_CONTEXT), MATH_CONTEXT), MATH_CONTEXT)
          .add(this.dmParams[4].multiply(cModel,
              MATH_CONTEXT), MATH_CONTEXT);
    }

    /**
     * Query model for all feedback documents.
     *
     * @param term Term to calculate the query model value for
     * @return Query model value for all feedback documents
     */
    BigDecimal query(final BytesRef term) {
      return Arrays.stream(this.feedbackDocs)
          .mapToObj(d -> document(this.docModels.get(d), term)
              .multiply(this.staticQueryModelParts.get(d),
                  MATH_CONTEXT))
          .reduce(BigDecimal.ZERO, (sum, qm) -> sum.add(qm, MATH_CONTEXT));
    }
  }

  /**
   * Class wrapping all methods needed for low-precision calculation of model
   * values. Also holds results of the calculations.
   */
  private static final class ModelLowPrecision
      extends AbstractModel {
    /**
     * Logger instance for this class.
     */
    private static final org.slf4j.Logger LOG =
        LoggerFactory.getLogger(ModelLowPrecision.class);
    /**
     * Stores the static part of the query model calculation for each document.
     */
    private final Map<Integer, Double> staticQueryModelParts;
    /**
     * Stores the static part of the smoothed model calculation for each
     * document.
     */
    private final Map<Integer, Double> staticSmoothingParts;
    /**
     * Document model calculation parameters: [0] Smoothing (mu), [1] Beta, [2]
     * Lambda, [3] 1 - Beta, [4] 1 - Lambda
     */
    private final double[] dmParams;

    /**
     * Initialize the model calculation object.
     *
     * @param dataProv DataProvider instance from parent class
     * @param qt Query terms. Query terms not found in the collection (TF=0)
     * will be skipped.
     * @param fb Feedback documents
     * @param dmSmoothing Document model: Smoothing parameter value
     * @param dmBeta Document model: Beta parameter value
     * @param dmLambda Document model: Lambda parameter value
     * @throws IOException Thrown on low-level I/O-errors
     */
    ModelLowPrecision(final IndexDataProvider dataProv,
        final BytesRefArray qt, final DocIdSet fb,
        final double dmSmoothing, final double dmBeta, final double dmLambda)
        throws IOException {
      super(dataProv, qt, fb);

      this.dmParams = new double[]{dmSmoothing, dmBeta, dmLambda,
          1d - dmBeta, 1d - dmLambda};

      // initialize other properties
      this.staticQueryModelParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.length * 1.8));
      this.staticSmoothingParts = new ConcurrentHashMap<>((int) (
          (double) this.feedbackDocs.length * 1.8));

      LOG.info("Pre-calculating static query model and smoothing values");
      for (final Integer docId : this.feedbackDocs) {
        final DocumentModel docModel = this.dataProv.getDocumentModel(docId);

        // smoothing value
        final double sSmooth = (double) docModel.tf() +
            (this.dmParams[0] * (double) docModel.termCount());
        this.staticSmoothingParts.put(docId, sSmooth);

        // static query model part (needs smoothing values)
        final double staticPart = StreamUtils.stream(this.queryTerms)
            .mapToDouble(br -> document(docModel, br))
            .reduce(1d, (g, c) -> g * c);

        this.staticQueryModelParts.put(docId, staticPart);
      }
    }

    /**
     * Document model.
     *
     * @param docModel Document data model
     * @param term Term to calculate the document model value for
     * @return Document model value
     */
    double document(
        final DocumentModel docModel, final BytesRef term) {
      // collection model of current term
      final double cModel = this.dataProv.getRelativeTermFrequency(term);

      // smoothed document-term model
      final double smoothingPart = this.staticSmoothingParts.get(docModel.id);
      final double smoothing;
      if (smoothingPart == 0d) {
        smoothing = 0;
      } else {
        smoothing = ((double) docModel.tf(term) + (this.dmParams[0] *
            cModel)) / this.staticSmoothingParts.get(docModel.id);
      }

      // final model calculation
      return (this.dmParams[2] * ((this.dmParams[1] * smoothing) +
          (this.dmParams[3] * cModel))) + (this.dmParams[4] * cModel);
    }

    /**
     * Query model for all feedback documents.
     *
     * @param term Term to calculate the query model value for
     * @return Query model value for all feedback documents
     */
    double query(final BytesRef term) {
      return Arrays.stream(this.feedbackDocs)
          .mapToDouble(d -> document(this.docModels.get(d), term) *
              this.staticQueryModelParts.get(d)).sum();
    }
  }

  /**
   * Create a new instance using a builder.
   *
   * @param builder Builder to use for constructing the instance
   */
  @SuppressWarnings("WeakerAccess")
  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
  ImprovedClarityScore(final Builder builder) {
    super(IDENTIFIER);
    Objects.requireNonNull(builder, "Builder was null.");

    // set configuration
    assert builder.getIndexDataProvider() != null;
    this.dataProv = builder.getIndexDataProvider();
    assert builder.getConfiguration() != null;
    this.conf = builder.getConfiguration();

    // check config
    if ((long) this.conf.getMinFeedbackDocumentsCount() >
        this.dataProv.getDocumentCount()) {
      throw new IllegalStateException(
          "Required minimum number of feedback documents ("
              + this.conf.getMinFeedbackDocumentsCount() + ") is larger "
              + "or equal compared to the total amount of indexed documents "
              + '(' + this.dataProv.getDocumentCount()
              + "). Unable to provide feedback."
      );
    }
    this.conf.debugDump();

    assert builder.getAnalyzer() != null;
    this.analyzer = builder.getAnalyzer();

    this.vocProvider = builder.getVocabularyProvider();
    this.vocProvider.indexDataProvider(this.dataProv);

    this.fbProvider = builder.getFeedbackProvider();
    assert builder.getIndexReader() != null;
    this.fbProvider
        .dataProvider(this.dataProv)
        .indexReader(builder.getIndexReader())
        .analyzer(this.analyzer);
  }

  /**
   * Calculates the improved clarity score for a given query.
   *
   * @param query Query to calculate for
   * @return Clarity score result object
   */
  @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
  @Override
  public Result calculateClarity(@NotNull final String query)
      throws ClarityScoreCalculationException {
    if (StringUtils.isStrippedEmpty(query)) {
      throw new IllegalArgumentException("Query was empty.");
    }

    // result object
    final Result result = new Result();
    boolean resultNotEmpty = true;

    // get a normalized unique list of query terms
    // skips stopwords and removes unknown terms (not visible in current
    // fields, etc.)
    final BytesRefArray queryTerms = QueryUtils.tokenizeQuery(query,
        this.analyzer, this.dataProv);
    // check query term extraction result
    if (queryTerms.size() == 0) {
      result.setEmpty(EmptyReason.NO_QUERY_TERMS);
      return result;
    }

    // save base data to result object
    result.setConf(this.conf);
    result.setQueryTerms(queryTerms);

    LOG.info("Calculating clarity score. query={}", query);
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    // set of feedback documents to use for calculation.
    DocIdSet feedbackDocIds;
    int fbDocCount;
    try {
      feedbackDocIds = this.fbProvider
          .query(queryTerms)
          .fields(this.dataProv.getDocumentFields())
          .unboundAmount()
//          .amount(
//              this.conf.getMinFeedbackDocumentsCount(),
//              this.conf.getMaxFeedbackDocumentsCount())
          .get();
      fbDocCount = DocIdSetUtils.cardinality(feedbackDocIds);
    } catch (final TooManyClauses e) {
      resultNotEmpty = false;
      feedbackDocIds = EMPTY_DOCIDSET;
      fbDocCount = 0;
      result.setEmpty(EmptyReason.TOO_MANY_BOOLCLAUSES);
    } catch (final Exception e) {
      final String msg = "Caught exception while getting feedback documents.";
      LOG.error(msg, e);
      throw new ClarityScoreCalculationException(msg, e);
    }

    if (resultNotEmpty) {
      if (fbDocCount == 0) {
        resultNotEmpty = false;
        result.setEmpty(EmptyReason.NO_FEEDBACK);
      } else if (fbDocCount < this.conf.getMinFeedbackDocumentsCount()) {
        resultNotEmpty = false;
        result.setEmpty("Not enough feedback documents. " +
            this.conf.getMinFeedbackDocumentsCount() +
            " requested, " + fbDocCount + " retrieved.");
      }
    }

    if (resultNotEmpty) {
      LOG.info("Parsing feedback documents");
      final ArrayList<Integer> matched = new ArrayList<>(
          this.conf.getMaxFeedbackDocumentsCount());
      try {
        final Map<Long, ArrayList<Integer>> reduced = reduceFeedbackDocuments(
            collectFeedbackDocuments(queryTerms, feedbackDocIds));

        // check, if we need more documents to get the lowest required amount
        if (!reduced.isEmpty()) {
          final List<Long> matchOrder = new ArrayList<>(reduced.keySet());
          Collections.sort(matchOrder); // sort by match count (asc)
          Collections.reverse(matchOrder); // reverse to desc order

          // step down by number of matched terms
          long current;
          while (!matchOrder.isEmpty() &&
              matched.size() < this.conf.getMinFeedbackDocumentsCount()) {
            current = matchOrder.get(0); // pop top element (highest)
            matched.addAll(reduced.get(current));

            // store current value
            result.setMatchingTermsCount(current);

            // remove those results we've added
            matchOrder.remove(0);
          }
        }
      } catch (final IOException e) {
        final String msg = "Caught exception while reducing feedback " +
            "documents.";
        LOG.error(msg, e);
        throw new ClarityScoreCalculationException(msg, e);
      }

      int newFbDocCount = 0;
      if (!matched.isEmpty()) {
        newFbDocCount = matched.size();
        // reduce feedback amount, if we gathered too much
        if (matched.size() > this.conf.getMaxFeedbackDocumentsCount()) {
          matched
              .subList(this.conf.getMaxFeedbackDocumentsCount(), newFbDocCount)
              .clear();
          newFbDocCount = matched.size();
        }

        // store results
        final BitSet fbDocBits = new FixedBitSet(Collections.max(matched) + 1);
        matched.stream().forEach(fbDocBits::set);
        feedbackDocIds = new BitDocIdSet(fbDocBits);
        result.setFeedbackDocIds(feedbackDocIds);

        LOG.info("Number of feedback documents reduced from {} to {}.",
            fbDocCount, newFbDocCount);
      }

      if (newFbDocCount < this.conf.getMinFeedbackDocumentsCount()) {
        resultNotEmpty = false;
        result.setEmpty("Not enough feedback documents. " +
            this.conf.getMinFeedbackDocumentsCount() +
            " requested, " + newFbDocCount + " retrieved and reduced.");
      }
    }

    if (resultNotEmpty) {
      // get document frequency threshold - allowed terms must be in bounds
      final double minFreq = this.conf
          .getMinFeedbackTermSelectionThreshold();
      final double maxFreq = this.conf
          .getMaxFeedbackTermSelectionThreshold();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Feedback term Document frequency threshold: {}%-{}%",
            minFreq * 100d, maxFreq * 100d);
      }

      // feedback terms stream used for calculation
      final Stream<BytesRef> fbTermStream = this.vocProvider
          .documentIds(feedbackDocIds)
          .get()
              // filter common terms
          .filter(t -> {
            final double relDf = this.dataProv.getRelativeDocumentFrequency(t);
            return relDf >= minFreq && relDf <= maxFreq;
          });

      try {
        final AtomicBoolean hasTerms = new AtomicBoolean(false);
        if (MATH_LOW_PRECISION) {
          final ModelLowPrecision model = new ModelLowPrecision(
              this.dataProv, queryTerms, feedbackDocIds,
              this.conf.getDocumentModelSmoothingParameter(),
              this.conf.getDocumentModelParamBeta(),
              this.conf.getDocumentModelParamLambda());

          LOG.info("Calculating query models using feedback vocabulary. " +
              "(low precision)");
          final ScoreTupleLowPrecision[] dataSets = fbTermStream
              .peek(term -> hasTerms.set(true))
              .map(term -> new ScoreTupleLowPrecision(
                  model.query(term),
                  this.dataProv.getRelativeTermFrequency(term), term))
              .toArray(ScoreTupleLowPrecision[]::new);

          if (hasTerms.get()) {
            LOG.info("Calculating final score.");
            result.setScoringTuple(dataSets);
            result.setScore(MathUtils.klDivergence(dataSets));
          } else {
            LOG.info(MSG_NO_THRESHOLDED_FEEDBACK_TERMS);
            resultNotEmpty = false;
            result.setEmpty(MSG_NO_THRESHOLDED_FEEDBACK_TERMS);
          }
        } else {
          final ModelHighPrecision model = new ModelHighPrecision(
              this.dataProv, queryTerms, feedbackDocIds,
              this.conf.getDocumentModelSmoothingParameter(),
              this.conf.getDocumentModelParamBeta(),
              this.conf.getDocumentModelParamLambda());

          LOG.info("Calculating query models using feedback vocabulary. " +
              "(high precision)");
          final ScoreTupleHighPrecision[] dataSets = fbTermStream
              .peek(term -> hasTerms.set(true))
              .map(term -> new ScoreTupleHighPrecision(
                  model.query(term), BigDecimal.valueOf(
                  this.dataProv.getRelativeTermFrequency(term)), term))
              .toArray(ScoreTupleHighPrecision[]::new);

          if (hasTerms.get()) {
            LOG.info("Calculating final score.");
            result.setScoringTuple(dataSets);
            result.setScore(MathUtils.klDivergence(dataSets).doubleValue());
          } else {
            LOG.info(MSG_NO_THRESHOLDED_FEEDBACK_TERMS);
            resultNotEmpty = false;
            result.setEmpty(MSG_NO_THRESHOLDED_FEEDBACK_TERMS);
          }
        }
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    if (LOG.isDebugEnabled()) {
      if (resultNotEmpty) {
        LOG.debug("Calculating default clarity score for query '{}' "
                + "with {} document models took {}. score={}",
            query, fbDocCount, timeMeasure.stop().getTimeString(),
            result.getScore());
      } else {
        String msg = "Calculating improved clarity score for query {} "
            + "with {} document models took {}. Result is empty.";
        if (result.getEmptyReason().isPresent()) {
          msg += " reason=" + result.getEmptyReason().get();
        }
        LOG.debug(msg, query, fbDocCount, timeMeasure.stop().getTimeString());
      }
    }

    return result;
  }

  private Map<Long, ArrayList<Integer>> reduceFeedbackDocuments(
      @NotNull final FeedbackDocumentCollector fdc) {
    final Map<Long, ArrayList<Integer>> reduced = new HashMap<>(
        this.conf.getMaxFeedbackDocumentsCount());

    for (Tuple2<Integer, Long> t2 : fdc.getOrderedCollection()) {
      if (reduced.containsKey(t2.b)) {
        reduced.get(t2.b).add(t2.a);
      } else {
        final ArrayList<Integer> docIds =
            new ArrayList<>(this.conf.getMaxFeedbackDocumentsCount() / 10);
        docIds.add(t2.a);
        reduced.put(t2.b, docIds);
      }
    }

    return reduced;
  }

  private FeedbackDocumentCollector collectFeedbackDocuments(
      @NotNull final BytesRefArray queryTerms,
      @NotNull final DocIdSet fbDocsIds)
      throws IOException {
    // number of query terms
    final BytesRefHash qtHash = BytesRefUtils.arrayToHash(queryTerms);
    final int queryLength = qtHash.size();

    final FeedbackDocumentCollector fbdCollector =
        new FeedbackDocumentCollector(this.conf.getMaxFeedbackDocumentsCount());

    final DocIdSetIterator disi = fbDocsIds.iterator();
    int docId = disi.nextDoc();

    if (queryLength == 1) {
      // special case for single term queries: take first documents that have
      // the required term and skip the rest.
      final BytesRef queryTerm = queryTerms.iterator().next();

      // check all feedback documents..
      while (docId != DocIdSetIterator.NO_MORE_DOCS) {
        // .. using their model ..
        final DocumentModel docMod = this.dataProv.getDocumentModel(docId);

        if (docMod.tf(queryTerm) > 0L) {
          // term found!
          fbdCollector.addDocument(docMod.id, 1L);
        }

        // collect up to the maximum
        if (fbdCollector.numEntries ==
            this.conf.getMaxFeedbackDocumentsCount()) {
          break;
        }
        docId = disi.nextDoc();
      }
    } else {
      // check all feedback documents..
      while (docId != DocIdSetIterator.NO_MORE_DOCS) {
        // .. using their model ..
        final DocumentModel docMod = this.dataProv.getDocumentModel(docId);

        // only check, if there are enough terms in document
        if ((long) docMod.termCount() > fbdCollector.lowerBound) {
          final int matchingTerms =
              // .. how many query terms the contain
              StreamUtils.stream(qtHash)
                  // term frequency == 0 means term not found
                  .mapToLong(docMod::tf)
                      // count only matches, not their value
                  .mapToInt(tf -> tf == 0L ? 0 : 1)
                  .sum();

          fbdCollector.addDocument(docMod.id, (long) matchingTerms);

          if (fbdCollector.isFull() &&
              fbdCollector.lowerBound == (long) queryLength) {
            // all collected documents matching all query terms .. it can't get
            // any better, so we stop here
            break;
          }
        }

        docId = disi.nextDoc();
      }
    }

    return fbdCollector;
  }

  /**
   * Collect feedback documents based on the number of matching terms.
   */
  private static final class FeedbackDocumentCollector {
    /**
     * Lowest number of matching terms currently stored
     */
    long lowerBound;
    /**
     * Highest number of matching terms currently stored.
     */
    long upperBound;
    /**
     * Maximum number of elements to store.
     */
    final int maxEntries;
    /**
     * Current number of elements in store.
     */
    int numEntries;

    /**
     * Collected document ids. Array index refers to matches array.
     */
    final ArrayList<Integer> docIds;
    /**
     * Collected number of matches. Array index refers to docIds array.
     */
    final ArrayList<Long> matches;

    /**
     *
     * @param size Maximum number of documents to collect
     */
    FeedbackDocumentCollector(final int size) {
      this.maxEntries = size;
      this.docIds = new ArrayList<>(size);
      this.matches = new ArrayList<>(size);
    }

    private void updateState() {
      this.numEntries = this.docIds.size();
      this.lowerBound = Collections.min(this.matches);
      this.upperBound = Collections.min(this.matches);
    }

    /**
     * Check current fill state.
     * @return true, if all slots are filled with values
     */
    boolean isFull() {
      return this.numEntries == this.maxEntries;
    }

    ArrayList<Tuple2<Integer, Long>> getOrderedCollection() {
      final ArrayList<Tuple2<Integer, Long>> coll =
          new ArrayList<>(this.numEntries);
      for (int i = 0; i < this.numEntries; i++) {
        coll.add(Tuple.tuple2(this.docIds.get(i), this.matches.get(i)));
      }
      return coll;
    }

    void addDocument(final int docId, final long matchingTerms) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Collect doc={} matches={} lBound={} uBound={}",
            docId, matchingTerms, this.lowerBound, this.upperBound);
      }

      boolean addDoc = false;
      if (matchingTerms <= this.lowerBound && !isFull()) {
        // add low value, there's still space left
        addDoc = true;
      } else if (matchingTerms > this.lowerBound) {
        // already full, only add bigger values
        if (isFull()) {
          // remove worst matching document
          final int worseMatchIndex = this.matches.lastIndexOf(this.lowerBound);
          this.matches.remove(worseMatchIndex);
          this.docIds.remove(worseMatchIndex);
        }
        addDoc = true;
      }

      // push the new values
      if (addDoc) {
        this.docIds.add(docId);
        this.matches.add(matchingTerms);
        updateState();
      }
    }
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
    @Nullable
    private ImprovedClarityScoreConfiguration conf;

    /**
     * Ids of feedback documents used for calculation.
     */
    private Optional<BitSet> feedbackDocIds = Optional.empty();
    private Long[] matchingTermsCount = {0L, 0L};

    /**
     * Creates an object wrapping the result with meta information.
     */
    public Result() {
      super(ImprovedClarityScore.class);
    }

    /**
     * Scoring tuple values for each feedback term. (High precision)
     */
    private Optional<ScoreTupleHighPrecision[]> stHighPrecision =
        Optional.empty();

    /**
     * Scoring tuple values for each feedback term. (Low precision)
     */
    private Optional<ScoreTupleLowPrecision[]> stLowPrecision =
        Optional.empty();

    /**
     * Map of document-id -> matching query terms in document count
     */
    private Optional<Map<Integer, Long>> feedbackDocMap = Optional.empty();

    /**
     * Set the list of feedback documents used.
     *
     * @param fbDocIds List of feedback documents
     */
    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    void setFeedbackDocIds(@NotNull final DocIdSet fbDocIds) {
      try {
        this.feedbackDocIds = Optional.of(DocIdSetUtils.bits(fbDocIds));
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    void setFeedbackDocMap(@NotNull Map<Integer, Long> fbMap) {
      this.feedbackDocMap = Optional.of(fbMap);
    }

    Optional<Map<Integer, Long>> getFeedbackDocMap() {
      return this.feedbackDocMap;
    }

    void setScoringTuple(final ScoreTupleHighPrecision[] stHigh) {
      this.stHighPrecision = Optional.of(stHigh);
    }

    void setScoringTuple(final ScoreTupleLowPrecision[] stLow) {
      this.stLowPrecision = Optional.of(stLow);
    }

    public Optional<BitSet> getFeedbackDocIds() {
      return this.feedbackDocIds;
    }

    public Optional<ScoreTupleHighPrecision[]> getScoreTupleHighPrecision() {
      return this.stHighPrecision;
    }

    public Optional<ScoreTupleLowPrecision[]> getScoreTupleLowPrecision() {
      return this.stLowPrecision;
    }

    public Optional<TupleType> getScoreTuple() {
      if (this.stHighPrecision.isPresent()) {
        return Optional.of(TupleType.HIGH_PRECISION);
      } else if (this.stLowPrecision.isPresent()) {
        return Optional.of(TupleType.LOW_PRECISION);
      } else {
        return Optional.empty();
      }
    }

    /**
     * Set the configuration that was used.
     *
     * @param newConf Configuration used
     */
    void setConf(@NotNull final ImprovedClarityScoreConfiguration newConf) {
      this.conf = newConf;
    }

    /**
     * Get the configuration used for this calculation result.
     *
     * @return Configuration used for this calculation result
     */
    @Nullable
    public ImprovedClarityScoreConfiguration getConfiguration() {
      return this.conf;
    }

    public void setMatchingTermsCount(final long mtc) {
      if (this.matchingTermsCount[0] == 0L &&
          this.matchingTermsCount[0].compareTo(
              this.matchingTermsCount[1]) == 0) {
        // set initial value
        this.matchingTermsCount[0] = mtc;
        this.matchingTermsCount[1] = mtc;
      } else {
        if (mtc > this.matchingTermsCount[1]) {
          this.matchingTermsCount[1] = mtc;
        } else if (mtc < this.matchingTermsCount[0]) {
          this.matchingTermsCount[0] = mtc;
        }
      }
    }

    public Long[] getMatchingTermsCounts() {
      return this.matchingTermsCount;
    }
  }

  /**
   * Builder to create a new {@link ImprovedClarityScore} instance.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractCSCBuilder<Builder, ImprovedClarityScore> {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(Builder.class);

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    ImprovedClarityScoreConfiguration getConfiguration() {
      if (this.conf == null) {
        LOG.info("Using default configuration.");
        return new ImprovedClarityScoreConfiguration();
      }
      return (ImprovedClarityScoreConfiguration) this.conf;
    }

    @Override
    public ImprovedClarityScore build()
        throws BuildableException {
      validateFeatures(Feature.CONFIGURATION, Feature.ANALYZER,
          Feature.DATA_PROVIDER, Feature.INDEX_READER);
      validateConfiguration(ImprovedClarityScoreConfiguration.class);
      return new ImprovedClarityScore(this);
    }
  }
}
