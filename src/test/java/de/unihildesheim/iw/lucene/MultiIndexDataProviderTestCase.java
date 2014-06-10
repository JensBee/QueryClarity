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
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import de.unihildesheim.iw.util.RandomValue;
import org.junit.After;
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
   * Test documents referenceIndex.
   */
  @SuppressWarnings({"ProtectedField", "StaticNonFinalField"})
  protected static TestIndexDataProvider referenceIndex;
  /**
   * Index configuration type.
   */
  private final RunType runType;
  /**
   * DataProvider type.
   */
  private final DataProviders dpType;
  /**
   * Current DataProvider instance being tested.
   */
  @SuppressWarnings("ProtectedField")
  protected IndexDataProvider index;

  /**
   * Initialize the generic test case.
   *
   * @param dataProv DataProvider
   * @param rType DataProvider configuration
   */
  protected MultiIndexDataProviderTestCase(final DataProviders dataProv,
      final RunType rType) {
    assert dataProv != null;
    assert rType != null;
    this.dpType = dataProv;
    this.runType = rType;
  }

  /**
   * Static initializer run before all tests.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @BeforeClass
  public static void setUpClass()
      throws Exception {
    referenceIndex = new TestIndexDataProvider();
  }

  /**
   * Run after all tests have finished.
   */
  @SuppressWarnings("StaticVariableUsedBeforeInitialization")
  @AfterClass
  public static void tearDownClass() {
    // close the test referenceIndex
    referenceIndex.close();
  }

  /**
   * Get parameters for parameterized test.
   *
   * @return Test parameters
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    final Collection<Object[]> params = new ArrayList<>(DataProviders.values
        ().length);

    for (final DataProviders dp : DataProviders.values()) {
      for (final RunType r : RunType.values()) {
        params.add(new Object[]{dp, r});
      }
    }
    return params;
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
        fields = referenceIndex.util().getRandomFields();
        stopwords = Collections.emptySet();
        break;
      case RANDOM_FIELDS_AND_STOPPED:
        fields = referenceIndex.util().getRandomFields();
        stopwords = referenceIndex.util().getRandomStopWords();
        break;
      case STOPPED:
        fields = referenceIndex.reference().getDocumentFields();
        stopwords = referenceIndex.util().getRandomStopWords();
        break;
      case PLAIN:
      default:
        fields = referenceIndex.reference().getDocumentFields();
        stopwords = Collections.emptySet();
        break;
    }

    referenceIndex.prepareTestEnvironment(fields, stopwords);

    switch (this.dpType) {
      case DIRECT:
        this.index = new DirectIndexDataProvider.Builder()
            .temporary()
            .documentFields(fields)
            .stopwords(stopwords)
            .dataPath(referenceIndex.reference().getDataDir())
            .indexPath(referenceIndex.reference().getIndexDir())
            .indexReader(TestIndexDataProvider.getIndexReader())
            .createCache("test-" + RandomValue.getString(16))
            .warmUp() // important!
            .build();
        break;
      default:
        this.index = referenceIndex;
        break;
    }
    referenceIndex.warmUp();
    LOG.info("MutilindexDataProviderTestCase SetUp finished "
            + "dataProvider={} configuration={}",
        this.dpType.name(), this.runType.name()
    );
  }

  @After
  public final void tearDown() {
    if (this.index != null) {
      this.index.close();
    }
  }

  /**
   * Prepend a message string with the current {@link IndexDataProvider} name
   * and the testing type.
   *
   * @param msg Message to prepend
   * @return Message prepended with testing information
   */
  protected final String msg(final String msg) {
    return "(" + getDataProviderName() + ", " + this.runType.name() + ", " +
        TestIndexDataProvider.getIndexConfName() + ") " + msg;
  }

  /**
   * Get the name of the {@link IndexDataProvider} currently in use.
   *
   * @return DataProvider name
   */
  protected final String getDataProviderName() {
    return this.index.getClass().getCanonicalName();
  }

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

  /**
   * Data provider types.
   */
  @SuppressWarnings("ProtectedInnerClass")
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
}
