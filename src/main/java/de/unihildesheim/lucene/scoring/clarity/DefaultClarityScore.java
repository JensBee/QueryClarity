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
package de.unihildesheim.lucene.scoring.clarity;

import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.Feedback;
import de.unihildesheim.lucene.document.TermDataManager;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.query.QueryUtils;
import de.unihildesheim.util.BytesWrap;
import de.unihildesheim.util.TimeMeasure;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.slf4j.LoggerFactory;

/**
 * Default Clarity Score implementation as defined by Cronen-Townsend, Steve,
 * Yun Zhou, and W. Bruce Croft.
 *
 * Reference:
 *
 * “Predicting Query Performance.” In Proceedings of the 25th Annual
 * International ACM SIGIR Conference on Research and Development in Information
 * Retrieval, 299–306. SIGIR ’02. New York, NY, USA: ACM, 2002.
 * doi:10.1145/564376.564429.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DefaultClarityScore implements ClarityScoreCalculation {

  /**
   * Prefix to use to store calculated term-data values in cache and access
   * properties stored in the {@link DataProvider}.
   */
  private static final String PREFIX = "DCS";

  /**
   * Keys to store calculation results in document models and access properties
   * stored in the {@link DataProvider}.
   */
  private enum DataKeys {

    /**
     * Stores the document model for a specific term in a {@link DocumentModel}.
     */
    DOC_MODEL,
    /**
     * Flag to indicate, if all document-models have already been
     * pre-calculated. Stored in the {@link IndexDataProvider}.
     */
    DOCMODELS_PRECALCULATED
  }

  /**
   * Default multiplier value for relative term frequency inside documents.
   */
  private static final double DEFAULT_LANGMODEL_WEIGHT = 0.6d;

  /**
   * Multiplier for relative term frequency inside documents.
   */
  private double langmodelWeight = DEFAULT_LANGMODEL_WEIGHT;

  /**
   * Allowed delta when resetting language model weights.
   */
  private static final double LANGMODEL_EPSILON = 0.000000000000001;

  /**
   * Default number of feedback documents to use. Cronen-Townsend et al.
   * recommend 500 documents.
   */
  private static final int DEFAULT_FEDDBACK_DOCS_COUNT = 500;

  /**
   * Number of feedback documents to use.
   */
  private int fbDocCount = DEFAULT_FEDDBACK_DOCS_COUNT;

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          DefaultClarityScore.class);

  /**
   * Provider for statistical index related informations.
   */
  private final IndexDataProvider dataProv;

  /**
   * Index reader used by this instance.
   */
  private final IndexReader reader;

  /**
   * {@link TermDataManager} to access to extended data storage for
   * {@link DocumentModel} data storage.
   */
  private final TermDataManager tdMan;

  /**
   * Default constructor using the {@link IndexDataProvider} for statistical
   * index data.
   *
   * @param indexReader {@link IndexReader} to use by this instance
   * @param dataProvider Provider for statistical index data
   */
  public DefaultClarityScore(final IndexReader indexReader,
          final IndexDataProvider dataProvider) {
    super();
    this.reader = indexReader;
    this.dataProv = dataProvider;
    this.tdMan = new TermDataManager(PREFIX, this.dataProv);
  }

  /**
   * Calculates the default value, if the term is not contained in document.
   * This value is also part of the regular calculation formula.
   *
   * @param term Term whose model to calculate
   * @return The calculated default model value
   */
  private double calcDefaultDocumentModel(final BytesWrap term) {
    return (double) (1 - langmodelWeight) * dataProv.getRelativeTermFrequency(
            term);
  }

  /**
   * Calculate the document model for the given term.
   *
   * @param docModel Document data model to use
   * @param term Term do do the calculation for
   * @param update If true, value will be written to the documents data model
   * @return Calculated model value
   */
  private double calcDocumentModel(final DocumentModel docModel,
          final BytesWrap term, final boolean update) {
    // no value was stored, so calculate it
    final double model = langmodelWeight * ((double) docModel.getTermFrequency(
            term) / (double) docModel.getTermFrequency())
            + calcDefaultDocumentModel(term);
    // update document model
    if (update) {
      this.tdMan.setTermData(docModel, term, DataKeys.DOC_MODEL.name(), model);
    }
    return model;
  }

  /**
   * Calculate the document language model for a given term.
   *
   * @param docModel Document model to do the calculation for
   * @param term Term to do the calculation for
   * @param force If true, the recalculation of the stored model values is
   * forced
   * @return Calculated language model for the given document and term
   */
  private double getDocumentModel(final DocumentModel docModel,
          final BytesWrap term, final boolean force) {
    Double model = null;

    if (docModel.containsTerm(term)) {
      if (!force) {
        // try to get the already calculated value
        model = (Double) this.tdMan.getTermData(docModel, term,
                DataKeys.DOC_MODEL.name());
      }

      if (force || model == null) {
        // no value was stored, so calculate and store it
        model = calcDocumentModel(docModel, term, true);
      }
    } else {
      // term not in document
      model = calcDefaultDocumentModel(term);
    }
    return model;
  }

  /**
   * Calculate the weighting value for all terms in the query.
   *
   * @param docModels Document models to use for calculation
   * @param queryTerms Terms of the originating query
   * @return Mapping of {@link DocumentModel} to calculated language modelString
   */
  private Map<DocumentModel, Double> calculateQueryModelWeight(
          final Set<DocumentModel> docModels,
          final BytesRef[] queryTerms) {
    final Map<DocumentModel, Double> weights = new HashMap(docModels.size());
    @SuppressWarnings("UnusedAssignment")
    double modelWeight;

    for (DocumentModel docModel : docModels) {
      modelWeight = 1d;
      for (BytesRef term : queryTerms) {
        modelWeight *= getDocumentModel(docModel, BytesWrap.wrap(term.bytes),
                false);
      }
      weights.put(docModel, modelWeight);
    }
    return weights;
  }

  /**
   * Pre-calculate all document models for all terms known from the index.
   *
   * Forcing a recalculation is needed, if the language model weight has changed
   * by calling {@link DefaultClarityScore#setLangmodelWeight(double)}.
   *
   * @param force If true, the recalculation is forced
   */
  public void preCalcDocumentModels(final boolean force) {
    final int termsCount = this.dataProv.getTermsCount();
    LOG.info("Pre-calculating document models ({}) "
            + "for all unique terms ({}) in index.",
            this.dataProv.getDocModelCount(), termsCount);

    final TimeMeasure timeMeasure = new TimeMeasure().start();
    final Iterator<DocumentModel> docModelsIt = this.dataProv.
            getDocModelIterator();
    Iterator<BytesWrap> idxTermsIt;
    BytesWrap term;
    DocumentModel docModel;

    // debug helpers
    int[] dbgStatus = new int[]{-1, 100, this.dataProv.getDocModelCount()};
    TimeMeasure dbgTimeMeasure = null;
    if (LOG.isDebugEnabled() && dbgStatus[2] > dbgStatus[1]) {
      dbgStatus[0] = 0;
      dbgTimeMeasure = new TimeMeasure();
    }

    while (docModelsIt.hasNext()) {
      // remove model from known list to modify it
      docModel = this.dataProv.
              removeDocumentModel(docModelsIt.next().getDocId());
      docModel.unlock();

      // debug operating indicator
      if (dbgStatus[0] >= 0 && ++dbgStatus[0] % dbgStatus[1] == 0) {
        LOG.debug("{} models of {} terms calculated ({}s)", dbgStatus[0],
                dbgStatus[2], dbgTimeMeasure.stop().getElapsedSeconds());
        dbgTimeMeasure.start();
      }

      idxTermsIt = this.dataProv.getTermsIterator();
      while (idxTermsIt.hasNext()) {
        term = idxTermsIt.next();
        if (docModel.containsTerm(term)) {
          calcDocumentModel(docModel, term, true);
        }
      }

      // re-add modified model to known list
      docModel.lock();
      this.dataProv.addDocumentModel(docModel);
    }

    timeMeasure.stop();
    LOG.info("Pre-calculating document models for all unique terms in index "
            + "took {} seconds", timeMeasure.getElapsedSeconds());
    // store that we have pre-calculated values
    this.dataProv.setProperty(PREFIX, DataKeys.DOCMODELS_PRECALCULATED.
            name(), "true");
  }

  /**
   * Calculate the clarity score.
   *
   * @param docModels Document models to use for calculation
   * @param idxTermsIt Iterator over all terms from the index
   * @param queryTerms Terms contained in the originating query
   * @return Result of the calculation
   */
  private ClarityScoreResult calculateClarity(
          final Set<DocumentModel> docModels,
          final Iterator<BytesWrap> idxTermsIt,
          final BytesRef[] queryTerms) {
    final TimeMeasure timeMeasure = new TimeMeasure().start();
    double score = 0d;
    double log;
    double qLangMod;

    LOG.debug("Calculating clarity score query={}", (Object[]) queryTerms);

    Map<DocumentModel, Double> modelWeights = calculateQueryModelWeight(
            docModels, queryTerms);

    // iterate over all terms in index
    BytesWrap term;
    while (idxTermsIt.hasNext()) {
      term = idxTermsIt.next();

      // calculate the query probability of the current term
      qLangMod = 0d;
      for (DocumentModel docModel : docModels) {
        qLangMod += getDocumentModel(docModel, term, false) * modelWeights.get(
                docModel);
      }

      // calculate logarithmic part of the formular
      log = (Math.log(qLangMod) / Math.log(2)) / (Math.log(
              dataProv.getRelativeTermFrequency(term)) / Math.log(2));
      // add up final score for each term
      score += qLangMod * log;
    }

    LOG.debug("Calculation results: query={} docModels={} score={} ({}).",
            queryTerms, docModels.size(), score, score);
//    LOG.debug("Calculation results: query={} docModels={} score={} ({}).",
//            queryTerms, docModels.size(), score, BigDecimal.valueOf(score).
//            toPlainString());

    final ClarityScoreResult result = new ClarityScoreResult(this.getClass(),
            score);

    timeMeasure.stop();
    LOG.debug("Calculating default clarity score for query {} "
            + "with {} document models took {} seconds.", queryTerms, docModels.
            size(), timeMeasure.getElapsedSeconds());

    return result;
  }

  /**
   * Same as {@link DefaultClarityScore#calculateClarity(Query)}, but allows to
   * pass in the list of feedback documents.
   *
   * @param query Query used for term extraction
   * @param fbDocIds List of document-ids to use for feedback calculation
   * @return Calculated clarity score for the given terms
   */
  public ClarityScoreResult calculateClarity(final Query query,
          final Integer[] fbDocIds) {
    if (query == null) {
      throw new IllegalArgumentException("Query was null.");
    }
    if (fbDocIds == null || fbDocIds.length == 0) {
      throw new IllegalArgumentException("No feedback documents given.");
    }

    ClarityScoreResult result;

    // check if document models are pre-calculated and stored
    final boolean hasPrecalcData = Boolean.parseBoolean(this.dataProv.
            getProperty(PREFIX, DataKeys.DOCMODELS_PRECALCULATED.name()));
    if (hasPrecalcData) {
      LOG.info("Using pre-calculated document models.");
    } else {
      // document models have to be calculated - this is not a must, but is a
      // good idea (performance-wise)
      preCalcDocumentModels(false);
    }

    final Set<DocumentModel> docModels = new HashSet(fbDocIds.length);

    for (Integer docId : fbDocIds) {
      docModels.add(this.dataProv.getDocumentModel(docId));
    }

    try {
      result = calculateClarity(docModels, this.dataProv.getTermsIterator(),
              QueryUtils.getQueryTerms(reader, query));
    } catch (IOException ex) {
      result = new ClarityScoreResult(this.getClass());
      LOG.error("Caught exception while calculating clarity score.", ex);
    }

    return result;
  }

  @Override
  public ClarityScoreResult calculateClarity(final Query query) {
    if (query == null) {
      throw new IllegalArgumentException("Query was null.");
    }

    ClarityScoreResult result;
    try {
      // get feedback documents..
      final Integer[] fbDocIds = Feedback.getFixed(reader, query,
              this.fbDocCount);
      // ..and calculate score
      result = calculateClarity(query, fbDocIds);
    } catch (IOException ex) {
      LOG.error("Error while trying to get feedback documents.", ex);
      // return an empty result on errors
      result = new ClarityScoreResult(this.getClass());
    }

    return result;
  }

  /**
   * Get the number of feedback documents that should be used for calculation.
   * The actual number of documents used depends on the amount of document
   * available in the index.
   *
   * @return Number of feedback documents that should be used for calculation
   */
  public int getFbDocCount() {
    return fbDocCount;
  }

  /**
   * Set the number of feedback documents that should be used for calculation.
   * The actual number of documents used depends on the amount of document
   * available in the index.
   *
   * @param feedbackDocCount Number of feedback documents to use for
   * calculation. This value must be greater than <tt>0</tt>.
   */
  public void setFbDocCount(final int feedbackDocCount) {
    if (feedbackDocCount <= 0) {
      throw new IllegalArgumentException(
              "Number of feedback documents mst be greater than zero.");
    }
    this.fbDocCount = feedbackDocCount;
  }

  /**
   * Get the weighting value used for document language model calculation.
   *
   * @return Weighting value
   */
  public double getLangmodelWeight() {
    return langmodelWeight;
  }

  /**
   * Set the weighting value used for document language model calculation. The
   * value should always be lower than <tt>1</tt>.
   *
   * Please note that you may have to recalculate any previously calculated and
   * cached values. This can be done by calling
   * {@link DefaultClarityScore#preCalcDocumentModels(boolean)} and setting
   * <code>force</code> to true.
   *
   * @param newLangmodelWeight New weighting value
   */
  public void setLangmodelWeight(final double newLangmodelWeight) {
    if (newLangmodelWeight > 1 || newLangmodelWeight <= 0) {
      throw new IllegalArgumentException(
              "Language model weight should be greater between 0-1.");
    }
    // check, if weight has changed
    if ((Math.abs(this.langmodelWeight - newLangmodelWeight) < LANGMODEL_EPSILON)) {
      LOG.info("Language model weight has changed. "
              + "You may need to recalculate any possibly cached values.");
    }
    this.langmodelWeight = newLangmodelWeight;
  }
}
