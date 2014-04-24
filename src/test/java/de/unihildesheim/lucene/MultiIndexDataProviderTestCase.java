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
package de.unihildesheim.lucene;

import de.unihildesheim.lucene.index.DirectIndexDataProvider;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.IndexTestUtil;
import de.unihildesheim.lucene.index.TestIndexDataProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test case utils for testing with multiple {@link IndexDataProvider}s.
 */
public class MultiIndexDataProviderTestCase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          MultiIndexDataProviderTestCase.class);

  /**
   * DataProvider instance currently in use.
   */
  private final Class<? extends IndexDataProvider> dataProvType;

  /**
   * Index configuration type.
   */
  private final RunType runType;

  /**
   * Test documents index.
   */
  protected static TestIndexDataProvider index;

  protected enum RunType {

    /**
     * Use full index.
     */
    PLAIN,
    /**
     * Activate random fields and use stopwords.
     */
    RANDOM_FIELDS_AND_STOPPED,
    /**
     * Activate random fields only.
     */
    RANDOM_FIELDS,
    /**
     * Use stopwords.
     */
    STOPPED,

  }

  /**
   * Initialize the generic test case.
   *
   * @param dataProv DataProvider
   * @param rType DataProvider configuration
   */
  protected MultiIndexDataProviderTestCase(
          final Class<? extends IndexDataProvider> dataProv,
          final RunType rType) {
    this.dataProvType = dataProv;
    this.runType = rType;
  }

  /**
   * Initialize the generic test case.
   *
   * @param dataProv DataProvider
   */
  protected MultiIndexDataProviderTestCase(
          final Class<? extends IndexDataProvider> dataProv) {
    this.dataProvType = dataProv;
    this.runType = RunType.PLAIN;
  }

  /**
   * Get the list of {@link IndexDataProvider}s to test.
   *
   * @return List of {@link IndexDataProvider}s
   */
  protected static final Collection<
        Class<? extends IndexDataProvider>> getDataProvider() {
    final Collection<Class<? extends IndexDataProvider>> providers
            = new ArrayList<>(2);
    providers.add(DirectIndexDataProvider.class);
    return providers;
  }

  /**
   * Get the list of {@link IndexDataProvider}s for parameterized tests.
   *
   * @return List of {@link IndexDataProvider}s
   */
  protected static final Collection<Object[]> getCaseParameters() {
    final Collection<Class<? extends IndexDataProvider>> providers
            = getDataProvider();
    final Collection<Object[]> params = new ArrayList<>(providers.size());

    for (Class<? extends IndexDataProvider> prov : getDataProvider()) {
      for (RunType r : RunType.values()) {
        params.add(new Object[]{prov, r});
      }
    }
    for (RunType r : RunType.values()) {
      params.add(new Object[]{null, r});
    }
    return params;
  }

  /**
   * Setup the {@link IndexDataProvider} for a test.
   *
   * @throws IOException Any exception indicates an error
   */
  protected final void caseSetUp() throws Exception {
    Environment.clear();
    Environment.clearAllProperties();
    LOG.info("MutilindexDataProviderTestCase SetUp "
            + "dataProvider={} configuration={}",
            this.dataProvType == null ? "TestIndexDataProvider"
            : this.dataProvType, this.runType.name());
    Collection<String> fields = null;
    Collection<String> stopwords = null;
    switch (this.runType) {
      case RANDOM_FIELDS:
        fields = IndexTestUtil.getRandomFields(index);
        break;
      case RANDOM_FIELDS_AND_STOPPED:
        fields = IndexTestUtil.getRandomFields(index);
        stopwords = IndexTestUtil.getRandomStopWords(index);
        break;
      case STOPPED:
        stopwords = IndexTestUtil.getRandomStopWords(index);
        break;
      case PLAIN:
      default:
        break;
    }
    if (this.dataProvType == null) {
      index.setupEnvironment(fields, stopwords);
    } else {
      index.setupEnvironment(this.dataProvType, fields, stopwords);
    }
    index.warmUp();
    Environment.getDataProvider().warmUp();
    LOG.info("MutilindexDataProviderTestCase SetUp finished "
            + "dataProvider={} configuration={}",
            this.dataProvType == null ? "TestIndexDataProvider"
            : this.dataProvType, this.runType.name());
  }

  /**
   * Get the name of the {@link IndexDataProvider} currently in use.
   *
   * @return DataProvider name
   */
  protected final String getDataProviderName() {
    return Environment.getDataProvider().getClass().getCanonicalName() + " "
            + runType.name();
  }
}
