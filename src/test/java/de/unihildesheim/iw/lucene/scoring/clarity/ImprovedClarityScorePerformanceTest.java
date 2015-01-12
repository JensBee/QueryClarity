/*
 * Copyright (C) 2014 Jens Bertram
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

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.index.DataProviderException;
import de.unihildesheim.iw.lucene.index.DirectAccessIndexDataProvider;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.IndexTestUtils;
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import de.unihildesheim.iw.lucene.scoring.data.DefaultFeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Jens Bertram
 */
public class ImprovedClarityScorePerformanceTest extends TestCase {

  /**
   * Logger instance for this class.
   */
  static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      ImprovedClarityScorePerformanceTest.class);
  private final int testRuns = 3;
  /**
   * Data provider.
   */
  private final IndexDataProvider dataProv;
  /**
   * Fixed configuration for clarity calculation.
   */
  private final ImprovedClarityScoreConfiguration iccConf;

  private final FeedbackProvider fbProv;

  public ImprovedClarityScorePerformanceTest()
      throws Buildable.ConfigurationException, Buildable.BuildException,
             IOException, DataProviderException {
    final TestIndexDataProvider testIndex = new TestIndexDataProvider();
    this.dataProv = new DirectAccessIndexDataProvider
        .Builder()
        .documentFields(testIndex.getDocumentFields())
        .indexPath(TestIndexDataProvider.getIndexDir())
        .stopwords(testIndex.getStopwords())
        .build();

    // static configuration to match pre-calculated values
    this.iccConf = new ImprovedClarityScoreConfiguration();
    this.iccConf.setMaxFeedbackDocumentsCount(
        (int) this.dataProv.getDocumentCount());
    this.iccConf.setFeedbackTermSelectionThreshold(0d, 1d); // includes all
    this.iccConf.setDocumentModelParamBeta(0.6);
    this.iccConf.setDocumentModelParamLambda(1d);
    this.iccConf.setDocumentModelSmoothingParameter(100d);
    this.iccConf.setMinFeedbackDocumentsCount(1);

    this.fbProv = new DefaultFeedbackProvider();
    this.fbProv
        .analyzer(IndexTestUtils.getAnalyzer())
        .fields(testIndex.getDocumentFields())
        .indexReader(TestIndexDataProvider.getIndexReader());
  }

  /**
   * Get an instance builder for the {@link ImprovedClarityScore} loaded with
   * default values.
   *
   * @return Builder initialized with default values
   * @throws IOException Thrown on low-level I/O errors related to the Lucene
   * index
   */
  private ImprovedClarityScore.Builder getInstanceBuilder()
      throws IOException {
    return (ImprovedClarityScore.Builder) new ImprovedClarityScore.Builder()
        .indexDataProvider(this.dataProv)
        .dataPath(TestIndexDataProvider.getDataDir())
        .indexReader(TestIndexDataProvider.getIndexReader())
        .createCache("test-" + RandomValue.getString(16))
        .analyzer(IndexTestUtils.getAnalyzer())
        .temporary();
  }

  @Test
  public void score()
      throws Exception {
    // some random terms from the index will make up a query
    final int queryTermCount = 10;
    final Set<String> qTerms = new HashSet<>(queryTermCount);
    final Set<ByteArray> docTerms = this.dataProv.getDocumentTerms(RandomValue
        .getInteger(0, (int) this.dataProv.getDocumentCount())).keySet();

    for (final ByteArray docTerm : docTerms) {
      qTerms.add(ByteArrayUtils.utf8ToString(docTerm));
      if (qTerms.size() >= queryTermCount) {
        break;
      }
    }
    // create a query string from the list of terms
    final String queryStr = StringUtils.join(qTerms, " ");

    // use all documents for feedback
    final Set<Integer> fbDocIds = new HashSet<>((int) this.dataProv
        .getDocumentCount());
    final Iterator<Integer> idxDocIds = this.dataProv.getDocumentIds();
    while (idxDocIds.hasNext()) {
      fbDocIds.add(idxDocIds.next());
    }

    ImprovedClarityScore.Builder iBuilder = (ImprovedClarityScore.Builder)
        getInstanceBuilder()
            .feedbackProvider(this.fbProv
              .query(queryStr)
              .amount(
                  this.iccConf.getMinFeedbackDocumentsCount(),
                  this.iccConf.getMaxFeedbackDocumentsCount()))
            .configuration(this.iccConf);

    final TimeMeasure tm = new TimeMeasure();
    final double[] runTime = new double[this.testRuns];

    try (ImprovedClarityScore instance = iBuilder.build()) {
      for (int i=0; i< this.testRuns; i++) {
        tm.stop().start();
        instance.calculateClarity(queryStr);
        runTime[i] = tm.stop().getElapsedMillis();
      }
    }

    LOG.debug("Results (ms): {}", runTime);
  }
}
