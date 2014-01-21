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
import de.unihildesheim.util.TimeMeasure;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
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
  private double getDefaultDocumentModel(final String term) {
    return (double) (1 - langmodelWeight) * dataProv.getRelativeTermFrequency(
            term);
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
          final String term, final boolean force) {
    Double model;

    if (docModel.containsTerm(term)) {
      // try to get the already calculated value
      model = (Double) this.tdMan.getTermData(docModel, term,
              DataKeys.DOC_MODEL.toString());

      if (model == null || force) {
        // no value was stored, so calculate it
        model = langmodelWeight * ((double) docModel.getTermFrequency(term)
                / (double) docModel.getTermFrequency())
                + getDefaultDocumentModel(term);
        // update document model
        this.tdMan.setTermData(docModel, term,
                DataKeys.DOC_MODEL.toString(), model);
      }
    } else {
      // term not in document
      model = getDefaultDocumentModel(term);
    }

    return model;
  }

  /**
   * Calculate the weighting value for all terms in the query.
   *
   * @param docModels Document models to use for calculation
   * @param queryTerms Terms of the originating query
   * @return Mapping of {@link DocumentModel} to calculated language model
   */
  private Map<DocumentModel, Double> calculateQueryModelWeight(
          final Set<DocumentModel> docModels,
          final String[] queryTerms) {
    final Map<DocumentModel, Double> weights = new HashMap(docModels.size());
    @SuppressWarnings("UnusedAssignment")
    double modelWeight = 1d;
    Double docLangModelCollection; // language model for collection term

    for (DocumentModel docModel : docModels) {
      modelWeight = 1d;
      for (String term : queryTerms) {
        docLangModelCollection = getDocumentModel(docModel, term, false);
        modelWeight *= docLangModelCollection;
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
    final TimeMeasure timeMeasure = new TimeMeasure().start();
    LOG.info("Pre-calculating document models for all terms in index.");
    final Iterator<String> idxTermsIt = this.dataProv.getTermsIterator();

    while (idxTermsIt.hasNext()) {
      final String term = idxTermsIt.next();
      for (DocumentModel docModel : this.dataProv.getDocModels()) {
        getDocumentModel(docModel, term, force);
      }
    }
    timeMeasure.stop();
    LOG.info("Pre-calculating document models for all terms in index "
            + "took {} seconds", timeMeasure.getElapsedSeconds());
    // store that we have pre-calculated values
    this.dataProv.setProperty(PREFIX, DataKeys.DOCMODELS_PRECALCULATED.
            toString(), "true");
  }

  /**
   * Calculate the clarity score.
   * @param docModels Document models to use for calculation
   * @param idxTermsIt Iterator over all terms from the index
   * @param queryTerms Terms contained in the originating query
   * @return Result of the calculation
   */
  private ClarityScoreResult calculateClarity(
          final Set<DocumentModel> docModels, final Iterator<String> idxTermsIt,
          final String[] queryTerms) {
    final TimeMeasure timeMeasure = new TimeMeasure().start();
    double score = 0d;
    double log;
    double qLangMod;

    LOG.debug("Calculating clarity score query={}", (Object[]) queryTerms);

    Map<DocumentModel, Double> modelWeights = calculateQueryModelWeight(
            docModels, queryTerms);

    // iterate over all terms in index
    while (idxTermsIt.hasNext()) {
      final String term = idxTermsIt.next();

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
            queryTerms, docModels.size(), score, BigDecimal.valueOf(score).
            toPlainString());

    final ClarityScoreResult result = new ClarityScoreResult(this.getClass(),
            score);

    timeMeasure.stop();
    LOG.debug("Calculating default clarity score for query {} "
            + "with {} document models took {} seconds.", queryTerms, docModels.
            size(), timeMeasure.getElapsedSeconds());

    return result;
  }

  @Override
  public ClarityScoreResult calculateClarity(final Query query) {
    ClarityScoreResult result;

    Integer[] fbDocIds;

    // check if document models are pre-calculated and stored
    final boolean hasPrecalcData = Boolean.parseBoolean(this.dataProv.
            getProperty(
                    PREFIX, DataKeys.DOCMODELS_PRECALCULATED.toString()));
    if (hasPrecalcData) {
      LOG.info("Using pre-calculated document models.");
    } else {
      // document models have to be calculated - this is not a must, but is a
      // good idea (performance-wise)
      preCalcDocumentModels(false);
    }

    try {
      fbDocIds = Feedback.getFixed(reader, query, this.fbDocCount);
    } catch (IOException ex) {
      fbDocIds = new Integer[0];
      LOG.error("Error while trying to get feedback documents.", ex);
    }

    if (fbDocIds.length > 0) {
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
    } else {
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
   * @param feedbackDocCount Number of feedback documents to use for calculation
   */
  public void setFbDocCount(final int feedbackDocCount) {
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
    // check, if weight has changed
    if ((Math.abs(this.langmodelWeight - newLangmodelWeight) <
            LANGMODEL_EPSILON)) {
      LOG.info("Language model weight has changed. "
              + "You may need to recalculate any possibly cached values.");
    }
    this.langmodelWeight = newLangmodelWeight;
  }
}
