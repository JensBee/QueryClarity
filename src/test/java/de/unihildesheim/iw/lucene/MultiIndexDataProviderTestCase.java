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

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.index.DirectAccessIndexDataProvider;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.TestIndexDataProvider;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  @SuppressWarnings("ProtectedField")
  protected final TestIndexDataProvider referenceIndex;
  /**
   * Index configuration type.
   */
  private final RunType runType;
  /**
   * DataProvider type.
   */
  private final DataProviders dpType;

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
    this.referenceIndex = new TestIndexDataProvider();
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
   * Get a {@link IndexDataProvider} instance.
   *
   * @return New instance
   * @throws IOException Thrown on low-level I/O errors
   * @throws Buildable.BuildableException Thrown, if instance could not be
   * build
   */
  protected final IndexDataProvider getInstance()
      throws IOException, Buildable.BuildableException {
    final IndexDataProvider instance;
    LOG.info(
        "MutilindexDataProviderTestCase SetUp dataProvider={} configuration={}",
        this.dpType.name(), this.runType.name()
    );

    final Set<String> fields;
    final Set<String> stopwords;

    switch (this.runType) {
      case RANDOM_FIELDS:
        fields = this.referenceIndex.getRandomFields();
        stopwords = Collections.emptySet();
        break;
      case RANDOM_FIELDS_AND_STOPPED:
        fields = this.referenceIndex.getRandomFields();
        stopwords = this.referenceIndex.getRandomStopWords();
        break;
      case STOPPED:
        fields = TestIndexDataProvider.getAllDocumentFields();
        stopwords = this.referenceIndex.getRandomStopWords();
        break;
      case PLAIN:
      default:
        fields = TestIndexDataProvider.getAllDocumentFields();
        stopwords = Collections.emptySet();
        break;
    }

    this.referenceIndex.prepareTestEnvironment(fields, stopwords);

    switch (this.dpType) {
      case DIRECT_ACCESS:
        instance = new DirectAccessIndexDataProvider.Builder()
            .documentFields(fields)
            .stopwords(stopwords)
            .indexPath(TestIndexDataProvider.getIndexDir())
            .indexReader(TestIndexDataProvider.getIndexReader())
            .build();
        break;
      default:
        instance = this.referenceIndex;
        break;
    }
    this.referenceIndex.warmUp();
    LOG.info("MutilindexDataProviderTestCase SetUp finished "
            + "dataProvider={} configuration={}",
        this.dpType.name(), this.runType.name()
    );
    return instance;
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
    switch (this.dpType) {
      case DIRECT_ACCESS:
        return DirectAccessIndexDataProvider.class.getCanonicalName();
      default:
        return this.referenceIndex.getClass().getCanonicalName();
    }
  }

  /**
   * Prepend a message string with the given {@link IndexDataProvider} name and
   * the testing type.
   *
   * @param msg Message to prepend
   * @param dataProv IndexDataProvider to reference
   * @return Message prepended with testing information
   */
  protected final String msg(final String msg, final IndexDataProvider
      dataProv) {
    return "(" + dataProv.getClass().getCanonicalName() + ", " +
        this.runType.name() + ", " +
        TestIndexDataProvider.getIndexConfName() + ") " + msg;
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
     * {@link DirectAccessIndexDataProvider}
     */
    DIRECT_ACCESS
  }
}
