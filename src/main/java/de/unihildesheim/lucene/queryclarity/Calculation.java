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
package de.unihildesheim.lucene.queryclarity;

import de.unihildesheim.lucene.queryclarity.documentmodel.AbstractDocumentModel;
import de.unihildesheim.lucene.queryclarity.documentmodel.DocumentModelMap;
import de.unihildesheim.lucene.queryclarity.indexdata.AbstractIndexDataProvider;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.QueryTermExtractor;
import org.apache.lucene.search.highlight.WeightedTerm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class Calculation {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(Calculation.class);

  /**
   * Shared index reader instance.
   */
  private final IndexReader indexReader;

  /**
   * Maximum number of result documents to include for calculation.
   */
  private int feedbackDocCnt = 10;

  /**
   * Lucene index fields to operate on.
   */
  private String[] fields;

  private DocumentModelMap docsMap;

  /**
   * Data provider for cacheable index statistics.
   */
  private final AbstractIndexDataProvider dataProv;

  public Calculation(final AbstractIndexDataProvider dataProvider,
          final IndexReader reader, final String[] fieldNames) {
    this.dataProv = dataProvider;
    this.indexReader = reader;
    this.fields = fieldNames;
    docsMap = new DocumentModelMap(this.indexReader, this.dataProv);
  }

  /**
   * Close all open handles for this object. Call this, if the class is no
   * longer needed an should get garbage collected.
   *
   * @throws IOException Thrown if index could not be read
   */
  public void dispose() throws IOException {
    LOG.debug("Dispose: Closing lucene index");
    this.indexReader.close();
  }

  /**
   * Calculate the query probability (pQ(t)) based on a set of document models
   * and the query terms.
   *
   * @param docModels List of document models to use for calculation
   * @param queryTerms All terms contained in the query, forming the list of
   * document models
   * @param currentTerm Term to do the calculation for
   * @return Calculated query probability
   * @throws IOException Thrown if index could not be read
   */
  protected final Double calculateQueryProbability(
          final Collection<AbstractDocumentModel> docModels,
          final Set<String> queryTerms,
          final String currentTerm) throws IOException {
    Double probability = 0d;

    for (AbstractDocumentModel docModel : docModels) {
      probability += docModel.termProbability(currentTerm) * docModel.
              getProbabilityProduct(queryTerms);
    }

    LOG.debug("[pQ(t)] Q={} t={} p={} (using {} feedback documents)",
            queryTerms, currentTerm, probability, docModels.size());

    return probability;
  }

  /**
   * Calculate the clarity score for a query specified by the given terms,
   * taking into account the specified document models.
   *
   * @param terms Terms to use for calculation. This may be either all terms in
   * the collection or only the terms used in the (rewritten) query.
   * @param queryTerms Terms used in the original/rewritten query
   * @param docModels Document models to use for calculation
   * @return The query clarity score
   * @throws IOException Thrown if index could not be read
   */
  public final double calculateClarityScore(final Set<String> terms,
          final Set<String> queryTerms,
          final Collection<AbstractDocumentModel> docModels) throws IOException {
    double qProb;
    double score = 0d;
    double log;

    for (String term : terms) {
      qProb = calculateQueryProbability(docModels, queryTerms, term);
      log = (Math.log(qProb) / Math.log(2)) / (Math.log(
              dataProv.getRelativeTermFrequency(term)) / Math.log(2));
      score += qProb * log;
    }

    return score;
  }

  /**
   *
   * @param query User query to parse
   * @throws IOException Thrown if index could not be read
   */
  public final void calculateClarity(final Query query) throws IOException {
    final Query rwQuery = query.rewrite(this.indexReader);

    // get all terms from the query
    final WeightedTerm[] weightedQueryTerms = QueryTermExtractor.getTerms(
            rwQuery, true);

    // stores all plain terms from the weighted query terms
    final Set<String> queryTerms = new HashSet(weightedQueryTerms.length);

    // store all plain query terms
    for (WeightedTerm wTerm : weightedQueryTerms) {
      queryTerms.add(wTerm.getTerm());
    }

    final IndexSearcher searcher = new IndexSearcher(this.indexReader);

    LOG.info("Searching index query={}", query.toString());
    final TopDocs results = searcher.search(query, this.feedbackDocCnt);
    final int fbDocCnt = Math.min(results.totalHits, this.feedbackDocCnt);
    LOG.debug("Search results all={} maxDocs={}", results.totalHits,
            this.feedbackDocCnt);

    int docId;
    AbstractDocumentModel docModel;

    // remove previously calculated documents
    this.docsMap.clear();

    for (int i = 0; i < fbDocCnt; i++) {
      docId = results.scoreDocs[i].doc;
      LOG.info("--- docId={}", docId);

      // retrieve / create document model storage
      docModel = this.docsMap.put(docId);

      // loop to get document probabilities
      for (String term : queryTerms) {
        // get stemmed query term
        docModel.termProbability(term);
      }

      // debug (only needed, if all documents are gathered)
      if (LOG.isDebugEnabled()) {
        // calculate query probability based on current document probabilities
        for (String term : queryTerms) {
          this.calculateQueryProbability(this.docsMap.values(), queryTerms,
                  term);
        }
      }
    }

    double clarityScore = this.calculateClarityScore(this.dataProv.getTerms(),
            queryTerms, this.docsMap.values());
    LOG.info("Clarity score query={} models={} score={} ({})", query.toString(),
            this.docsMap.size(), clarityScore, BigDecimal.
            valueOf(clarityScore).toPlainString());
  }
}
