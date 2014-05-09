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
package de.unihildesheim.iw.lucene;

import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.index.DirectIndexDataProvider;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.IndexTestUtil;
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import de.unihildesheim.iw.util.RandomValue;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * Test case utilities for testing with multiple {@link IndexDataProvider}s.
 */
public class MultiIndexDataProviderTestCase
    extends TestCase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      MultiIndexDataProviderTestCase.class);

  /**
   * Index configuration type.
   */
  private final RunType runType;

  private final DataProviders dpType;

  /**
   * Test documents referenceIndex.
   */
  @SuppressWarnings("ProtectedField")
  protected static TestIndexDataProvider referenceIndex;

  protected IndexDataProvider index;

  /**
   * Types of configurations ran.
   */
  @SuppressWarnings("ProtectedInnerClass")
  protected enum RunType {

    /**
     * Use full referenceIndex.
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

  protected enum DataProviders {
    /**
     * {@link TestIndexDataProvider}
     */
    TEST,
    /**
     * {@link DirectIndexDataProvider}
     */
    DIRECT
  }

  /**
   * Initialize the generic test case.
   *
   * @param dataProv DataProvider
   * @param rType DataProvider configuration
   */
  protected MultiIndexDataProviderTestCase(DataProviders dataProv,
      final RunType rType) {
    this.dpType = dataProv;
    this.runType = rType;
  }

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static final void setUpClass()
      throws Exception {
    referenceIndex = new TestIndexDataProvider(TestIndexDataProvider
        .IndexSize.SMALL);
    assertTrue("TestIndex is not initialized.", TestIndexDataProvider.
        isInitialized());
  }

  /**
   * Run after all tests have finished.
   */
  @AfterClass
  public static final void tearDownClass() {
    // close the test referenceIndex
    referenceIndex.dispose();
  }

  /**
   * Run before each test starts.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Before
  public final void setUp()
      throws Exception {
    LOG.info(
        "MutilindexDataProviderTestCase SetUp dataProvider={} configuration={}",
        this.dpType.name(), this.runType.name()
    );

    final Set<String> fields;
    final Set<String> stopwords;

    switch (this.runType) {
      case RANDOM_FIELDS:
        fields = IndexTestUtil.getRandomFields(referenceIndex);
        stopwords = Collections.<String>emptySet();
        break;
      case RANDOM_FIELDS_AND_STOPPED:
        fields = IndexTestUtil.getRandomFields(referenceIndex);
        stopwords = IndexTestUtil.getRandomStopWords(referenceIndex);
        break;
      case STOPPED:
        fields = referenceIndex.reference.getDocumentFields();
        stopwords = IndexTestUtil.getRandomStopWords(referenceIndex);
        break;
      case PLAIN:
      default:
        fields = referenceIndex.reference.getDocumentFields();
        stopwords = Collections.<String>emptySet();
        break;
    }

    referenceIndex.prepareTestEnvironment(fields, stopwords);

    switch (this.dpType) {
      case DIRECT:
        this.index = new DirectIndexDataProvider.Builder()
            .temporary()
            .documentFields(fields)
            .stopwords(stopwords)
            .dataPath(TestIndexDataProvider.reference.getDataDir())
            .indexPath(TestIndexDataProvider.reference.getIndexDir())
            .indexReader(this.referenceIndex.getIndexReader())
            .createCache("test-" + RandomValue.getString(16))
            .build();
        break;
      default:
        this.index = referenceIndex;
        break;
    }
    this.referenceIndex.warmUp();
    this.index.warmUp();
    LOG.info("MutilindexDataProviderTestCase SetUp finished "
            + "dataProvider={} configuration={}",
        this.dpType.name(), this.runType.name()
    );
  }

  /**
   * Get parameters for parameterized test.
   *
   * @return Test parameters
   */
  @Parameterized.Parameters
  public static final Collection<Object[]> data() {
    final Collection<Object[]> params = new ArrayList<>(DataProviders.values
        ().length);

    for (DataProviders dp : DataProviders.values()) {
      for (RunType r : RunType.values()) {
        params.add(new Object[]{dp, r});
      }
    }
    return params;
  }

  /**
   * Prepend a message string with the current {@link IndexDataProvider} name
   * and the testing type.
   *
   * @param msg Message to prepend
   * @return Message prepended with testing informations
   */
  protected final String msg(final String msg) {
    return "(" + getDataProviderName() + ", " + this.runType.name() + ") "
        + msg;
  }

//  /**
//   * Get the class of the {@link IndexDataProvider} currently in use.
//   *
//   * @return DataProvider class
//   */
//  protected final Class<? extends IndexDataProvider> getDataProviderClass() {
//    if (this.dataProvType == null) {
//      return TestIndexDataProvider.class;
//    }
//    return this.dataProvType;
//  }

  /**
   * Get the name of the {@link IndexDataProvider} currently in use.
   *
   * @return DataProvider name
   */
  protected final String getDataProviderName() {
    return this.index.getClass().getCanonicalName() + " " + runType.name();
  }
}
