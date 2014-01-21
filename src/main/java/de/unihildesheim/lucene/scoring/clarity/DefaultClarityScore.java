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
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Iterator;
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
public class DefaultClarityScore implements ClarityScoreCalculation {

  /**
   * Prefix to use to store calculated term-data values in cache.
   */
  private static final String TD_PREFIX = "DCS";

  /**
   * Keys to store calculation results in document models.
   */
  private enum TermDataKeys {

    /**
     * Stores the document model for a specific term.
     */
    DOC_MODEL
  }

  /**
   * Multiplier for relative term frequency inside documents.
   */
  private double langmodelWeight = 0.6d;

  /**
   * Number of feedback documents to use. Cronen-Townsend et al. recommend 500
   * documents.
   */
  private int fbDocCount = 500;

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          DefaultClarityScore.class);

  /**
   * Provider for statistical index related informations.
   */
  private IndexDataProvider dataProv;

  /**
   * Index reader used by this instance.
   */
  private IndexReader reader;

  /**
   * {@link TermDataManager} to access to extended data storage for
   * {@link DocumentModel} data storage.
   */
  private TermDataManager tdMan;

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
    this.tdMan = new TermDataManager(TD_PREFIX, this.dataProv);
  }

  /**
   * Calculate the document language model for a given term.
   *
   * @param docModel Document model to do the calculation for
   * @param term Term to do the calculation for
   * @return Calculated language model for the given document and term
   */
  private double getDocumentModel(final DocumentModel docModel,
          final String term) {
    Double model;
    // default value, if term is not contained in document,
    // also part of the regular formular
    final double defaultValue = (double) (1 - langmodelWeight) * dataProv.
            getRelativeTermFrequency(term);

    if (docModel.containsTerm(term)) {
      // try to get the already calculated value
      model = (Double) this.tdMan.getTermData(docModel, term,
              TermDataKeys.DOC_MODEL.toString());

      if (model == null) {
        // no value was stored, so calculate it
        model = langmodelWeight * ((double) docModel.getTermFrequency(term)
                / (double) docModel.getTermFrequency()) + defaultValue;
        // update document model
        this.tdMan.setTermData(docModel, term,
                TermDataKeys.DOC_MODEL.toString(), model);
      }
    } else {
      // term not in document
      model = defaultValue;
    }

    return model;
  }

  /**
   * Calculate the query language model.
   *
   * @param docModels Document models to use for calculation
   * @param queryTerms All terms from the original query
   * @param currTerm Current term to do the calculation for
   * @return Query language model for the given term
   */
  private double calcQueryLangModel(final Set<DocumentModel> docModels,
          final String[] queryTerms, final String currTerm) {
    double prob = 0d;
    double modelWeight = 1d;
    Double docLangModelCollection; // language model for collection term
    Double docLangModelTerm; // language model for current term

    for (DocumentModel docModel : docModels) {
      modelWeight = 1d;
      for (String term : queryTerms) {
        docLangModelCollection = getDocumentModel(docModel, term);
        modelWeight *= docLangModelCollection;
      }
      docLangModelTerm = getDocumentModel(docModel, currTerm);
      prob += docLangModelTerm * modelWeight;
    }

    LOG.trace("[pQ(t)] Q={} t={} p={} (using {} feedback documents)",
            queryTerms, currTerm, prob, docModels.size());

    return prob;
  }

  /**
   * Pre-calculate all document models for all terms known from the index.
   */
  public void precalcDocumentModels() {
    final Iterator<String> idxTermsIt = this.dataProv.getTermsIterator();

    while (idxTermsIt.hasNext()) {
      for (DocumentModel docModel : this.dataProv.getDocModels()) {

      }
    }
  }

  private ClarityScoreResult calculateClarity(
          final Set<DocumentModel> docModels, final Iterator<String> idxTermsIt,
          final String[] queryTerms) {
    final long startTime = System.nanoTime();
    double score = 0d;
    double log;
    double qLangMod;

    LOG.debug("Calculating clarity score query={}", queryTerms);

    // iterate over all terms in index
    while (idxTermsIt.hasNext()) {
      final String term = idxTermsIt.next();
      // calculate the query probability of the current term
      qLangMod = calcQueryLangModel(docModels, queryTerms, term);
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
    final double estimatedTime = (double) (System.nanoTime() - startTime)
            / 1000000000.0;
    LOG.debug("Calculating default clarity score for query {} "
            + "with {} document models took {} seconds.", queryTerms, docModels.
            size(), estimatedTime);
    return result;
  }

  @Override
  public final ClarityScoreResult calculateClarity(final Query query) {
    ClarityScoreResult result;

    Integer[] fbDocIds;

    try {
      fbDocIds = Feedback.getFixed(reader, query, this.fbDocCount); // NOPMD
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
   * Get the provider for statistical index related informations used by this
   * instance.
   *
   * @return Data provider used by this instance
   */
  public final IndexDataProvider getDataProv() {
    return dataProv;
  }

  /**
   * Set the provider for statistical index related informations used by this
   * instance.
   *
   * @param dataProvider Data provider to use by this instance
   */
  public final void setDataProv(final IndexDataProvider dataProvider) {
    this.dataProv = dataProvider;
  }

  /**
   * Get the {@link IndexReader} used by this instance.
   *
   * @return {@link IndexReader} used by this instance
   */
  public final IndexReader getReader() {
    return reader;
  }

  /**
   * Set the {@link IndexReader} used by this instance.
   *
   * @param indexReader {@link IndexReader} to use by this instance
   */
  public final void setReader(final IndexReader indexReader) {
    this.reader = indexReader;
  }

  /**
   * Get the number of feedback documents that should be used for calculation.
   * The actual number of documents used depends on the amount of document
   * available in the index.
   *
   * @return Number of feedback documents that should be used for calculation
   */
  public final int getFbDocCount() {
    return fbDocCount;
  }

  /**
   * Set the number of feedback documents that should be used for calculation.
   * The actual number of documents used depends on the amount of document
   * available in the index.
   *
   * @param feedbackDocCount Number of feedback documents to use for calculation
   */
  public final void setFbDocCount(final int feedbackDocCount) {
    this.fbDocCount = feedbackDocCount;
  }

  /**
   * Get the weighting value used for document language model calculation.
   *
   * @return Weighting value
   */
  public final double getLangmodelWeight() {
    return langmodelWeight;
  }

  /**
   * Set the weighting value used for document language model calculation. The
   * value should always be lower than <tt>1</tt>. This will remove any
   * calculated values from the used {@link DocumentModel} instances and forces
   * a recalculation when needed.
   *
   * @param newLangmodelWeight New weighting value
   */
//  public final void setLangmodelWeight(double newLangmodelWeight) {
//    // check, if weight has changed
//    if (newLangmodelWeight != this.langmodelWeight) {
//      LOG.info("Language model weight has changed. "
//              + "Removing all calculated values from document models.");
//
//      final Iterator<DocumentModel> docModelIt = this.dataProv.
//              getDocModelIterator();
//
//      // remove calculated values for each documentModel
//      while (docModelIt.hasNext()) {
//        final DocumentModel docModel = docModelIt.next();
//        docModel.clearTermData(TermDataKeys.DOC_MODEL.toString());
//      }
//    }
//
//    this.langmodelWeight = newLangmodelWeight;
//  }
}
