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
package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.index.AbstractIndexDataProviderTest
    .AbstractIndexDataProviderTestImpl;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import de.unihildesheim.iw.util.concurrent.processing.TestTargets;
import org.apache.lucene.index.IndexReader;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared test functions for {@link IndexDataProvider} implementing classes.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("AbstractClassExtendsConcreteClass")
public abstract class IndexDataProviderTestCase
    extends TestCase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      IndexDataProviderTestCase.class);
  /**
   * Global singleton instance of the test-index.
   */
  private static final FixedTestIndexDataProvider FIXED_INDEX =
      FixedTestIndexDataProvider.getInstance();
  /**
   * Test-Index to check results against.
   */
  @SuppressWarnings("PackageVisibleField")
  final TestIndexDataProvider referenceIndex;

  /**
   * Private empty constructor for utility class.
   *
   * @param aReferenceIndex TestIndex to check against
   */
  public IndexDataProviderTestCase(
      final TestIndexDataProvider aReferenceIndex) {
    this.referenceIndex = aReferenceIndex;
  }

  /**
   * Get the working directory used by these tests. Path is provided by the
   * {@link TestIndexDataProvider}.
   *
   * @return Working directory path
   */
  protected static String getDataPath() {
    return TestIndexDataProvider.getDataDir();
  }

  /**
   * Get the index reader used to access the reference test index.
   *
   * @return Index reader accessing the reference index
   * @throws IOException Thrown on low-level I/O errors
   */
  protected static IndexReader getIndexReader()
      throws IOException {
    return TestIndexDataProvider.getIndexReader();
  }

  /**
   * Test method for getTermFrequency method. Testing against {@link
   * FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance()) {
      return;
    }
    try (final IndexDataProvider instance = setupFixedInstanceTest()) {
      Assert.assertEquals(msg(instance, "Term frequency differs."),
          FIXED_INDEX.getTermFrequency(), instance.getTermFrequency());
    }
  }

  /**
   * Check, if the current {@link IndexDataProvider} is able to be tested
   * against the {@link FixedTestIndexDataProvider}.
   *
   * @return True, if testing is allowed
   */
  private boolean canTestAgainstFixedInstance() {
    final Class<? extends IndexDataProvider> instance = getInstanceClass();
    boolean canTest = true;

    if (canTest()) {
      if (instance.equals(TestIndexDataProvider.class)) {
        canTest = false;
      }
    } else {
      canTest = false;
    }

    if (!canTest) {
      LOG.warn(msg(instance, "Skipping test for DataProvider."));
    }

    return canTest;
  }

  /**
   * Setup the testing environment to test against the {@link
   * FixedTestIndexDataProvider} which can also acts as an index to other {@link
   * IndexDataProvider}s.
   *
   * @return Instance of the {@link IndexDataProvider} currently tested
   * @throws Exception Forwarded from testing instance
   */
  private IndexDataProvider setupFixedInstanceTest()
      throws Exception {
    return createInstance(
        FixedTestIndexDataProvider.DATA_DIR.getPath(),
        FixedTestIndexDataProvider.TMP_IDX.getReader(),
        FIXED_INDEX.getDocumentFields(),
        FIXED_INDEX.getStopwords()
    );
  }

  /**
   * Prepend a message string with the current {@link IndexDataProvider} name
   * and the testing type.
   *
   * @param instance Data-provider instance
   * @param msg Message to prepend
   * @return Message prepended with testing information
   */
  private static String msg(final IndexDataProvider instance,
      final String msg) {
    return msg(instance.getClass(), msg);
  }

  /**
   * Get the class of the currently tested instance.
   *
   * @return Class
   */
  protected abstract Class<? extends IndexDataProvider> getInstanceClass();

  /**
   * Check, if the current {@link IndexDataProvider} is available for testing.
   * Classes may be excluded here from testing.
   *
   * @return True, if the provider is available for testing
   */
  private boolean canTest() {
    final Class<? extends IndexDataProvider> instance = getInstanceClass();
    boolean canTest = true;
    if (instance.equals(AbstractIndexDataProviderTestImpl.class)) {
      LOG.warn(msg(instance, "Skipping test. Plain instance."));
      canTest = false;
    }
    return canTest;
  }

  /**
   * Prepend a message string with the current {@link IndexDataProvider} name
   * and the testing type.
   *
   * @param instance Data-provider instance class
   * @param msg Message to prepend
   * @return Message prepended with testing information
   */
  private static String msg(final Class<? extends IndexDataProvider> instance,
      final String msg) {
    return "(" + instance.getSimpleName() + ") " + msg;
  }

  /**
   * Creates a new {@link IndexDataProvider} instance of the type currently
   * tested.
   *
   * @param dataDir Path to the directory where the temporary lucene index is
   * located
   * @param reader Reader to access the Lucene index
   * @param fields Document fields to query
   * @param stopwords Stopwords to exclude in calculations
   * @return instance of the type currently tested
   * @throws Exception Forwarded from testing instance
   */
  protected abstract IndexDataProvider createInstance(final String dataDir,
      final IndexReader reader, final Set<String> fields,
      final Set<String> stopwords)
      throws Exception;

  /**
   * Test method for getTermFrequency method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetTermFrequency_0args(instance);
    }
  }

  /**
   * Setup the testing environment to test against the {@link
   * TestIndexDataProvider}.
   *
   * @param setup Instance setup type
   * @return Instance of the {@link IndexDataProvider} currently tested
   * @throws Exception Forwarded from testing instance
   */
  private IndexDataProvider setupInstanceForTesting(final Setup setup)
      throws Exception {

    Set<String> fields = null;
    Set<String> stopwords = null;
    switch (setup) {
      case PLAIN:
        break;
      case RANDFIELD:
        fields = this.referenceIndex.getRandomFields();
        break;
      case RANDFIELD_STOPPED:
        fields = this.referenceIndex.getRandomFields();
        stopwords = this.referenceIndex.getRandomStopWords();
        break;
      case STOPPED:
        stopwords = this.referenceIndex.getRandomStopWords();
        break;
    }
    this.referenceIndex.prepareTestEnvironment(fields, stopwords);
    return createInstance(
        TestIndexDataProvider.getDataDir(),
        TestIndexDataProvider.getIndexReader(),
        this.referenceIndex.getDocumentFields(),
        this.referenceIndex.getStopwords());
  }

  /**
   * Test method for getTermFrequency method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   */
  private void runTestGetTermFrequency_0args(final IndexDataProvider instance) {
    Assert.assertEquals(
        msg(instance, "Term frequency differs."),
        this.referenceIndex.getTermFrequency(),
        instance.getTermFrequency());
  }

  /**
   * Test method for getTermFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }

    final long unfilteredTf = this.referenceIndex.getTermFrequency();
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetTermFrequency_0args(instance);

      // check with stopwords
      final long filteredTf = this.referenceIndex.getTermFrequency();
      Assert.assertEquals(
          msg(instance, "Term frequency differs. plain=" + unfilteredTf
              + " filter=" + filteredTf + "."),
          this.referenceIndex.getTermFrequency(),
          instance.getTermFrequency()
      );

      Assert.assertNotEquals(
          msg(instance,
              "TF using stop-words should be lower than without. filter="
                  + filteredTf + " plain=" + unfilteredTf + "."
          ),
          filteredTf, unfilteredTf
      );
    }
  }

  /**
   * Test method for getTermFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__randField()
      throws Exception {
    if (!canTest()) {
      return;
    }

    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetTermFrequency_0args(instance);
    }
  }

  /**
   * Test method for getTermFrequency method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }

    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetTermFrequency_0args(instance);
    }
  }

  /**
   * Test of getTermFrequency method. Testing against {@link
   * FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_ByteArray__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance()) {
      return;
    }

    try (final IndexDataProvider instance = setupFixedInstanceTest()) {
      final Iterator<ByteArray> idxTermsIt = FIXED_INDEX.getTermsIterator();

      while (idxTermsIt.hasNext()) {
        final ByteArray idxTerm = idxTermsIt.next();
        Assert.assertEquals(
            msg(instance, "Term frequency differs. term=" + idxTerm),
            FIXED_INDEX.getTermFrequency(idxTerm),
            instance.getTermFrequency(idxTerm)
        );
      }
    }
  }

  /**
   * Test of getTermFrequency method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_ByteArray__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }

    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetTermFrequency_ByteArray(instance);
    }
  }

  /**
   * Test method for getTermFrequency method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  private void runTestGetTermFrequency_ByteArray(
      final IndexDataProvider instance) {
    final boolean excludeStopwords = this.referenceIndex.hasStopwords();
    final Iterator<ByteArray> idxTermsIt = this.referenceIndex
        .getTermsIterator();

    while (idxTermsIt.hasNext()) {
      final ByteArray idxTerm = idxTermsIt.next();
      Assert.assertEquals(
          msg(instance, "Term frequency differs (stopped: "
              + excludeStopwords + "). term=" + idxTerm),
          this.referenceIndex.getTermFrequency(idxTerm),
          instance.getTermFrequency(idxTerm)
      );

      if (excludeStopwords) {
        final Collection<ByteArray> stopwords =
            this.referenceIndex.getStopwordsBytes();
        Assert.assertFalse(
            msg(instance, "Stopword found in term list."),
            stopwords.contains(idxTerm));
      }
    }
  }

  /**
   * Test of getTermFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_ByteArray__stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }

    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetTermFrequency_ByteArray(instance);
    }
  }

  /**
   * Test of getTermFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_ByteArray__randField()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetTermFrequency_ByteArray(instance);
    }
  }

  /**
   * Test of getTermFrequency method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_ByteArray__randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetTermFrequency_ByteArray(instance);
    }
  }

  /**
   * Test of getRelativeTermFrequency method. Testing against {@link
   * FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance()) {
      return;
    }
    try (final IndexDataProvider instance = setupFixedInstanceTest()) {
      final Iterator<ByteArray> idxTermsIt = FIXED_INDEX.getTermsIterator();

      while (idxTermsIt.hasNext()) {
        final ByteArray idxTerm = idxTermsIt.next();
        Assert.assertEquals(
            msg(instance, "Relative term frequency differs. term=" + idxTerm),
            FIXED_INDEX.getRelativeTermFrequency(idxTerm),
            instance.getRelativeTermFrequency(idxTerm), 0d
        );
      }
    }
  }

  /**
   * Test of getRelativeTermFrequency method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetRelativeTermFrequency(instance);
    }
  }

  /**
   * Test method for getRelativeTermFrequency method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  private void runTestGetRelativeTermFrequency(
      final IndexDataProvider instance) {

    final boolean excludeStopwords = this.referenceIndex.hasStopwords();
    final Iterator<ByteArray> idxTermsIt = this.referenceIndex
        .getTermsIterator();

    while (idxTermsIt.hasNext()) {
      final ByteArray idxTerm = idxTermsIt.next();
      Assert.assertEquals(
          msg(instance, "Relative term frequency differs (stopped: "
              + excludeStopwords + "). term=" + idxTerm),
          this.referenceIndex.getRelativeTermFrequency(idxTerm),
          instance.getRelativeTermFrequency(idxTerm), 0d
      );

      if (excludeStopwords) {
        final Collection<ByteArray> stopwords =
            this.referenceIndex.getStopwordsBytes();
        Assert.assertFalse(
            msg(instance, "Stopword found in term list."),
            stopwords.contains(idxTerm));
      }
    }
  }

  /**
   * Test of getRelativeTermFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetRelativeTermFrequency(instance);
    }
  }

  /**
   * Test of getRelativeTermFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__randField()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetRelativeTermFrequency(instance);
    }
  }

  /**
   * Test of getRelativeTermFrequency method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetRelativeTermFrequency(instance);
    }
  }

  /**
   * Test of getTermsIterator method. Testing against {@link
   * FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsIterator__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance()) {
      return;
    }
    try (final IndexDataProvider instance = setupFixedInstanceTest()) {
      final Iterator<ByteArray> result = instance.getTermsIterator();

      int iterations = 0;
      while (result.hasNext()) {
        iterations++;
        result.next();
      }

      Assert.assertEquals(msg(instance, "Not all terms found while iterating."),
          instance.getUniqueTermsCount(), (long) iterations);
      Assert.assertEquals(
          msg(instance, "Different values for unique terms reported."),
          (long) FixedTestIndexDataProvider.KnownData.TERM_COUNT_UNIQUE,
          instance.getUniqueTermsCount());
    }
  }

  /**
   * Test of getTermsIterator method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsIterator__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetTermsIterator(instance);
    }
  }

  /**
   * Test method for getTermsIterator method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  private void runTestGetTermsIterator(final IndexDataProvider instance)
      throws Exception {
    final boolean excludeStopwords = this.referenceIndex.hasStopwords();
    final Iterator<ByteArray> result = instance.getTermsIterator();

    int iterations = 0;
    while (result.hasNext()) {
      iterations++;
      if (excludeStopwords) {
        final Collection<ByteArray> stopwords =
            this.referenceIndex.getStopwordsBytes();
        Assert.assertFalse(msg(instance, "Found stopword."),
            stopwords.contains(result.next()));
      } else {
        result.next();
      }
    }

    Assert.assertEquals(
        msg(instance, "Not all terms found while iterating."),
        instance.getUniqueTermsCount(), (long) iterations);
    Assert.assertEquals(
        msg(instance, "Different values for unique terms reported."),
        this.referenceIndex.getUniqueTermsCount(),
        instance.getUniqueTermsCount());
  }

  /**
   * Test of getTermsIterator method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsIterator__stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetTermsIterator(instance);
    }
  }

  /**
   * Test of getTermsIterator method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsIterator__randField()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetTermsIterator(instance);
    }
  }

  /**
   * Test of getTermsIterator method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsIterator__randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetTermsIterator(instance);
    }
  }

  /**
   * Test of getDocumentCount method. Testing against {@link
   * FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance()) {
      return;
    }
    try (final IndexDataProvider instance = setupFixedInstanceTest()) {
      Assert.assertEquals(
          msg(instance, "Different number of documents reported."),
          (long) FixedTestIndexDataProvider.KnownData.DOC_COUNT,
          instance.getDocumentCount());
    }
  }

  /**
   * Test of getDocumentCount method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetDocumentCount(instance);
    }
  }

  /**
   * Test of getDocumentCount method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   */
  private void runTestGetDocumentCount(final IndexDataProvider instance) {
    Assert.assertEquals(
        msg(instance, "Different number of documents reported."),
        this.referenceIndex.getDocumentCount(),
        instance.getDocumentCount());
  }

  /**
   * Test of getDocumentCount method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__randField()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetDocumentCount(instance);
    }
  }

  /**
   * Test of getDocumentCount method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetDocumentCount(instance);
    }
  }

  /**
   * Test of getDocumentCount method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetDocumentCount(instance);
    }
  }

  /**
   * Test of getDocumentModel method. Testing against {@link
   * FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentModel__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance()) {
      return;
    }
    try (final IndexDataProvider instance = setupFixedInstanceTest()) {
      final Iterator<Integer> docIdIt = FIXED_INDEX.getDocumentIdIterator();

      while (docIdIt.hasNext()) {
        final Integer docId = docIdIt.next();
        final DocumentModel iDocModel = instance.getDocumentModel(docId);
        final DocumentModel eDocModel = FIXED_INDEX.getDocumentModel(docId);

        if (!eDocModel.equals(iDocModel)) {
          for (final Entry<ByteArray, Long> e :
              eDocModel.getTermFreqMap().entrySet()) {
          }
          for (final Entry<ByteArray, Long> e :
              iDocModel.getTermFreqMap().entrySet()) {
          }
        }

        Assert.assertEquals(msg(instance, "Equals failed for docId=" + docId),
            eDocModel, iDocModel
        );
      }
    }
  }

  /**
   * Test of getDocumentModel method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentModel__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetDocumentModel(instance);
    }
  }

  /**
   * Test method for getDocumentModel method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   */
  private void runTestGetDocumentModel(final IndexDataProvider instance) {
    final boolean excludeStopwords = this.referenceIndex.hasStopwords();
    final Iterator<Integer> docIdIt = this.referenceIndex
        .getDocumentIdIterator();

    while (docIdIt.hasNext()) {
      final Integer docId = docIdIt.next();
      final DocumentModel iDocModel = instance.getDocumentModel(docId);
      final DocumentModel eDocModel =
          this.referenceIndex.getDocumentModel(docId);

      Assert.assertEquals(
          msg(instance, "Equals failed (stopped: " + excludeStopwords + ") " +
              "for docId=" + docId), eDocModel, iDocModel
      );

      if (excludeStopwords) {
        final Collection<ByteArray> stopwords =
            this.referenceIndex.getStopwordsBytes();
        for (final ByteArray term : iDocModel.getTermFreqMap().keySet()) {
          Assert.assertFalse(
              msg(instance, "Found stopword in model."),
              stopwords.contains(term));
        }
      }
    }
  }

  /**
   * Test of getDocumentModel method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentModel__stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetDocumentModel(instance);
    }
  }

  /**
   * Test of getDocumentModel method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentModel__randField()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetDocumentModel(instance);
    }
  }

  /**
   * Test of getDocumentModel method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentModel__randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetDocumentModel(instance);
    }
  }

  /**
   * Test of getDocumentIdIterator method. Testing against {@link
   * FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance()) {
      return;
    }
    try (final IndexDataProvider instance = setupFixedInstanceTest()) {
      final long docCount =
          (long) FixedTestIndexDataProvider.KnownData.DOC_COUNT;
      long docCountIt = 0L;
      final Iterator<Integer> result = instance.getDocumentIdIterator();

      while (result.hasNext()) {
        docCountIt++;
        result.next();
      }
      Assert.assertEquals(msg(instance, "Number of document ids differ."),
          docCount, docCountIt);
    }
  }

  /**
   * Test of getDocumentIdIterator method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetDocumentIdIterator(instance);
    }
  }

  /**
   * Test of getDocumentIdIterator method. Plain.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   */
  private void runTestGetDocumentIdIterator(final IndexDataProvider instance) {
    final long docCount = this.referenceIndex.getDocumentCount();
    long docCountIt = 0L;
    final Iterator<Integer> result = instance.getDocumentIdIterator();

    while (result.hasNext()) {
      docCountIt++;
      result.next();
    }
    Assert.assertEquals(
        msg(instance, "Number of document ids differ."),
        docCount, docCountIt);
  }

  /**
   * Test of getDocumentIdIterator method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetDocumentIdIterator(instance);
    }
  }

  /**
   * Test of getDocumentIdIterator method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator_randField()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetDocumentIdIterator(instance);
    }
  }

  /**
   * Test of getDocumentIdIterator method. Using random fields and stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator_randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetDocumentIdIterator(instance);
    }
  }

  /**
   * Test of getDocumentIdSource method. Testing against {@link
   * FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance()) {
      return;
    }
    try (final IndexDataProvider instance = setupFixedInstanceTest()) {
      final AtomicLong counter = new AtomicLong(0L);
      new Processing(
          new TargetFuncCall<>(
              instance.getDocumentIdSource(),
              new TestTargets.FuncCall<Integer>(counter)
          )
      ).process(FixedTestIndexDataProvider.KnownData.DOC_COUNT);

      Assert.assertEquals(
          msg(instance, "Not all items provided by source or processed by " +
              "target."),
          (long) FixedTestIndexDataProvider.KnownData.DOC_COUNT, counter.get()
      );
    }
  }

  /**
   * Test of getDocumentIdSource method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetDocumentIdSource(instance);
    }
  }

  /**
   * Test of getDocumentIdSource method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  private void runTestGetDocumentIdSource(final IndexDataProvider instance)
      throws Exception {

    final AtomicLong counter = new AtomicLong(0L);
    new Processing(
        new TargetFuncCall<>(
            instance.getDocumentIdSource(),
            new TestTargets.FuncCall<Integer>(counter)
        )
    ).process((int) this.referenceIndex.getDocumentCount());

    Assert.assertEquals(
        msg(instance, "Not all items provided by source or processed by " +
            "target."),
        this.referenceIndex.getDocumentCount(), counter.get()
    );
  }

  /**
   * Test of getDocumentIdSource method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetDocumentIdSource(instance);
    }
  }

  /**
   * Test of getDocumentIdSource method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource_randField()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetDocumentIdSource(instance);
    }
  }

  /**
   * Test of getDocumentIdSource method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource_randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetDocumentIdSource(instance);
    }
  }

  /**
   * Test of getUniqueTermsCount method. Testing against {@link
   * FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance()) {
      return;
    }
    try (final IndexDataProvider instance = setupFixedInstanceTest()) {
      Assert.assertEquals(
          msg(instance, "Unique term count values are different."),
          (long) FIXED_INDEX.getTermSet().size(),
          instance.getUniqueTermsCount());
    }
  }

  /**
   * Test of getUniqueTermsCount method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetUniqueTermsCount(instance);
    }
  }

  /**
   * Test of getUniqueTermsCount method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any Exception thrown indicates an error
   */
  private void runTestGetUniqueTermsCount(final IndexDataProvider instance)
      throws Exception {
    Assert.assertEquals(
        msg(instance, "Unique term count values are different."),
        (long) this.referenceIndex.getTermSet().size(),
        instance.getUniqueTermsCount());
  }

  /**
   * Test of getUniqueTermsCount method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetUniqueTermsCount(instance);
    }
  }

  /**
   * Test of getUniqueTermsCount method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__randField()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetUniqueTermsCount(instance);
    }
  }

  /**
   * Test of getUniqueTermsCount method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetUniqueTermsCount(instance);
    }
  }

  /**
   * Test of hasDocument method. Testing against {@link
   * FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testHasDocument__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance()) {
      return;
    }
    try (final IndexDataProvider instance = setupFixedInstanceTest()) {
      final Iterator<Integer> docIdIt = FIXED_INDEX.getDocumentIdIterator();

      while (docIdIt.hasNext()) {
        Assert.assertTrue(msg(instance, "Document not found."),
            instance.hasDocument(docIdIt.next()));
      }

      Assert.assertFalse(msg(instance, "Document should not be found."),
          instance.hasDocument(-1));
      Assert.assertFalse(msg(instance, "Document should not be found."),
          instance.hasDocument(FixedTestIndexDataProvider.KnownData.DOC_COUNT));
    }
  }

  /**
   * Test of hasDocument method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testHasDocument__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    final Iterator<Integer> docIdIt = this.referenceIndex
        .getDocumentIdIterator();
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      while (docIdIt.hasNext()) {
        Assert.assertTrue(msg(instance, "Document not found."),
            instance.hasDocument(docIdIt.next()));
      }

      Assert.assertFalse(msg(instance, "Document should not be found."),
          instance.hasDocument(-1));
      Assert.assertFalse(msg(instance, "Document should not be found."),
          instance.hasDocument((int) this.referenceIndex.getDocumentCount()));
    }
  }

  /**
   * Test of documentContains method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDocumentContains__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestDocumentContains(instance);
    }
  }

  /**
   * Test method for documentContains method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  private void runTestDocumentContains(final IndexDataProvider instance) {
    final boolean excludeStopwords = this.referenceIndex.hasStopwords();
    final Iterator<Integer> docIdIt = instance.getDocumentIdIterator();

    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final DocumentModel docModel = instance.getDocumentModel(docId);
      for (final ByteArray byteArray : docModel.getTermFreqMap().keySet()) {
        Assert.assertTrue(
            msg(instance, "Document contains term mismatch (stopped: " +
                excludeStopwords + ")."),
            instance.documentContains(docId, byteArray)
        );
        if (excludeStopwords) {
          final Collection<ByteArray> stopwords =
              this.referenceIndex.getStopwordsBytes();
          Assert.assertFalse(
              msg(instance, "Found stopword."),
              stopwords.contains(byteArray));
        }
      }
    }
  }

  /**
   * Test of documentContains method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDocumentContains__stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestDocumentContains(instance);
    }
  }

  /**
   * Test of documentContains method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDocumentContains__randField()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestDocumentContains(instance);
    }
  }

  /**
   * Test of documentContains method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDocumentContains__randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestDocumentContains(instance);
    }
  }

  /**
   * Test of getTermsSource method. Testing against {@link
   * FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance()) {
      return;
    }
    try (final IndexDataProvider instance = setupFixedInstanceTest()) {
      final AtomicLong counter = new AtomicLong(0L);
      new Processing(
          new TargetFuncCall<>(
              instance.getTermsSource(),
              new TestTargets.FuncCall<ByteArray>(counter)
          )
      ).process(FixedTestIndexDataProvider.KnownData.TERM_COUNT_UNIQUE);

      Assert.assertEquals(
          msg(instance, "Not all items provided by source or processed by " +
              "target."),
          (long) FIXED_INDEX.getTermSet().size(),
          (long) counter.intValue()
      );
    }
  }

  /**
   * Test of getTermsSource method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetTermsSource(instance);
    }
  }

  /**
   * Test of getTermsSource method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any Exception thrown indicates an error
   */
  private void runTestGetTermsSource(final IndexDataProvider instance)
      throws Exception {

    final AtomicLong counter = new AtomicLong(0L);
    new Processing(
        new TargetFuncCall<>(
            instance.getTermsSource(),
            new TestTargets.FuncCall<ByteArray>(counter)
        )
    ).process((int) this.referenceIndex.getUniqueTermsCount());

    Assert.assertEquals(
        msg(instance, "Not all items provided by source or processed by " +
            "target."),
        (long) this.referenceIndex.getTermSet().size(),
        (long) counter.intValue()
    );
  }

  /**
   * Test of getTermsSource method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetTermsSource(instance);
    }
  }

  /**
   * Test of getTermsSource method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__randField()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetTermsSource(instance);
    }
  }

  /**
   * Test of getTermsSource method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetTermsSource(instance);
    }
  }

  /**
   * Test of getDocumentsTermSet method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentsTermSet__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetDocumentsTermSet(instance);
    }
  }

  /**
   * Test method for getDocumentsTermSet method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  private void runTestGetDocumentsTermSet(final IndexDataProvider instance)
      throws Exception {
    final boolean excludeStopwords = this.referenceIndex.hasStopwords();
    final int docAmount = RandomValue.getInteger(2, (int) this.referenceIndex.
        getDocumentCount() - 1);
    final Collection<Integer> docIds = new HashSet<>(docAmount);

    for (int i = 0; i < docAmount; ) {
      if (docIds.add(RandomValue.getInteger(0, RandomValue.getInteger(2,
          (int) this.referenceIndex.getDocumentCount() - 1)))) {
        i++;
      }
    }

    final Collection<ByteArray> expResult =
        this.referenceIndex.getDocumentsTermSet(docIds);
    final Collection<ByteArray> result = instance.getDocumentsTermSet(docIds);

    Assert.assertEquals(
        msg(instance, "Not the same amount of terms retrieved (stopped: " +
            excludeStopwords + ")."),
        (long) expResult.size(), (long) result.size()
    );
    Assert.assertTrue(
        msg(instance, "Not all terms retrieved (stopped: " + excludeStopwords
            + ")."),
        expResult.containsAll(result)
    );

    if (excludeStopwords) {
      final Collection<ByteArray> stopwords =
          this.referenceIndex.getStopwordsBytes();
      for (final ByteArray term : result) {
        Assert.assertFalse(
            msg(instance, "Stopword found in term list."),
            stopwords.contains(term));
      }
    }
  }

  /**
   * Test of getDocumentsTermSet method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentsTermSet__stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetDocumentsTermSet(instance);
    }
  }

  /**
   * Test of getDocumentsTermSet method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentsTermSet__randField()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetDocumentsTermSet(instance);
    }
  }

  /**
   * Test of getDocumentsTermSet method. Using stopwords & random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentsTermSet__randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetDocumentsTermSet(instance);
    }
  }

  /**
   * Test of getDocumentFrequency method. Testing against {@link
   * FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance()) {
      return;
    }
    try (final IndexDataProvider instance = setupFixedInstanceTest()) {
      final Iterator<ByteArray> termsIt = FIXED_INDEX.getTermsIterator();
      while (termsIt.hasNext()) {
        final ByteArray term = termsIt.next();
        Assert.assertEquals(
            msg(instance, "Document frequency mismatch (" +
                ByteArrayUtils.utf8ToString(term) + ")."),
            (long) FIXED_INDEX.getDocumentFrequency(term),
            (long) instance.getDocumentFrequency(term)
        );
      }
    }
  }

  /**
   * Test of getDocumentFrequency method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__plain()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetDocumentFrequency(instance);
    }
  }

  /**
   * Test method for getDocumentFrequency method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  private void runTestGetDocumentFrequency(final IndexDataProvider instance) {
    final boolean excludeStopwords = this.referenceIndex.hasStopwords();

    for (final ByteArray term : this.referenceIndex.getTermSet()) {
      Assert.assertEquals(
          msg(instance, "Document frequency mismatch (stopped: " +
              excludeStopwords + ") (" + ByteArrayUtils.utf8ToString(term) +
              ")."),
          (long) this.referenceIndex.getDocumentFrequency(term),
          (long) instance.getDocumentFrequency(term)
      );

      if (excludeStopwords) {
        final Collection<ByteArray> stopwords =
            this.referenceIndex.getStopwordsBytes();
        Assert.assertFalse(
            msg(instance, "Found stopword in term list."),
            stopwords.contains(term));
      }
    }
  }

  /**
   * Test of getDocumentFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetDocumentFrequency(instance);
    }
  }

  /**
   * Test of getDocumentFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__randField()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetDocumentFrequency(instance);
    }
  }

  /**
   * Test of getDocumentFrequency method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__randField_stopped()
      throws Exception {
    if (!canTest()) {
      return;
    }
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetDocumentFrequency(instance);
    }
  }

  /**
   * Test of warmUp method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUp__plain()
      throws Exception {
    try (final IndexDataProvider instance =
             setupInstanceForTesting(Setup.PLAIN)) {
      instance.warmUp();
    }
  }

  /**
   * Test of warmUpDocumentFrequencies method (from {@link
   * AbstractIndexDataProvider}).
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUpDocumentFrequencies__plain()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }

    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.PLAIN)) {
      instance.warmUpDocumentFrequencies();
    }
  }

  /**
   * Check, if the current {@link IndexDataProvider} is extending the {@link
   * de.unihildesheim.iw.lucene.index.AbstractIndexDataProvider}
   *
   * @return True, if it is extending
   */
  private boolean canTestAbstractIdpMethods() {
    final Class<? extends IndexDataProvider> instance = getInstanceClass();
    boolean canTest = true;

    if (canTest()) {
      if (!AbstractIndexDataProvider.class.isAssignableFrom(instance)) {
        LOG.warn(msg(instance, "Skipping test. "
            + "No sub-class of AbstractIndexDataProvider."));
        canTest = false;
      }
    } else {
      canTest = false;
    }

    return canTest;
  }

  /**
   * Test of getDocumentIds method (from {@link AbstractIndexDataProvider}).
   * Testing against {@link FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIds__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance() || !canTestAbstractIdpMethods()) {
      return;
    }

    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider) setupFixedInstanceTest()) {

      final Collection<Integer> docIds = new ArrayList<>(instance.
          getDocumentIds());
      final Iterator<Integer> docIdIt = FIXED_INDEX.getDocumentIdIterator();
      while (docIdIt.hasNext()) {
        final Integer docId = docIdIt.next();
        Assert.assertTrue(msg(instance, "Doc-id was missing. docId=" + docId),
            docIds.remove(docId));
      }
      Assert.assertTrue(
          msg(instance, "Too much document ids provided by instance."),
          docIds.isEmpty());
    }
  }

  /**
   * Test of getDocumentIds method (from {@link AbstractIndexDataProvider}).
   * Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIds__plain()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.PLAIN)) {
      final Collection<Integer> docIds = new ArrayList<>(instance.
          getDocumentIds());
      final Iterator<Integer> docIdIt = this.referenceIndex
          .getDocumentIdIterator();
      while (docIdIt.hasNext()) {
        final Integer docId = docIdIt.next();
        Assert.assertTrue(msg(instance, "Doc-id was missing. docId=" + docId),
            docIds.remove(docId));
      }
      Assert.assertTrue(
          msg(instance, "Too much document ids provided by instance."),
          docIds.isEmpty());
    }
  }

  /**
   * Test of warmUpTerms method (from {@link AbstractIndexDataProvider}).
   * Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUpTerms__plain()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.PLAIN)) {
      instance.warmUpTerms();
    }
  }

  /**
   * Test of warmUpTerms method (from {@link AbstractIndexDataProvider}). Using
   * stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUpTerms__stopped()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.STOPPED)) {
      instance.warmUpTerms();
    }
  }

  /**
   * Test of warmUpTerms method (from {@link AbstractIndexDataProvider}). Using
   * random fields.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUpTerms__randField()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.RANDFIELD)) {
      instance.warmUpTerms();
    }
  }

  /**
   * Test of warmUpTerms method (from {@link AbstractIndexDataProvider}). Using
   * random fields and stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUpTerms__randField_stopped()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.STOPPED)) {
      instance.warmUpTerms();
    }
  }

  /**
   * Test of warmUpIndexTermFrequencies method (from {@link
   * AbstractIndexDataProvider}). Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testWarmUpIndexTermFrequencies__plain()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.PLAIN)) {
      instance.warmUpIndexTermFrequencies();
    }
  }

  /**
   * Test of warmUpIndexTermFrequencies method (from {@link
   * AbstractIndexDataProvider}). Using stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testWarmUpIndexTermFrequencies__stopped()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.STOPPED)) {
      instance.warmUpIndexTermFrequencies();
    }
  }

  /**
   * Test of warmUpIndexTermFrequencies method (from {@link
   * AbstractIndexDataProvider}). Using random fields.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testWarmUpIndexTermFrequencies__randField()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.RANDFIELD)) {
      instance.warmUpIndexTermFrequencies();
    }
  }

  /**
   * Test of warmUpIndexTermFrequencies method (from {@link
   * AbstractIndexDataProvider}). Using random fields and stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testWarmUpIndexTermFrequencies__randField_stopped()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      instance.warmUpIndexTermFrequencies();
    }
  }

  /**
   * Test of warmUpDocumentIds method (from {@link AbstractIndexDataProvider}).
   * Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUpDocumentIds__plain()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.PLAIN)) {
      instance.warmUpDocumentIds();
    }
  }

  /**
   * Test of getFieldId method, of class AbstractIndexDataProvider. Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetFieldId__plain()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetFieldId(instance);
    }
  }

  /**
   * Test method for getFieldId method, of class AbstractIndexDataProvider.
   *
   * @param instance Prepared instance to test
   */
  private void runTestGetFieldId(final AbstractIndexDataProvider instance) {
    for (final String fieldName : this.referenceIndex.getDocumentFields()) {
      final SerializableByte result = instance.getFieldId(fieldName);
      Assert.assertNotNull(msg(instance, "Field id was null."), result);
    }
  }

  /**
   * Test of getFieldId method, of class AbstractIndexDataProvider. Using random
   * fields.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetFieldId__randField()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetFieldId(instance);
    }
  }

  /**
   * Test for getTermFrequencyIgnoringStopwords method, of class
   * AbstractIndexDataProvider. Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void test_getTermFrequency__plain()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.PLAIN)) {
      runTest_getTermFrequency(instance);
    }
  }

  /**
   * Test method for getTermFrequencyIgnoringStopwords method, of class
   * AbstractIndexDataProvider.
   *
   * @param instance Prepared instance to test
   */
  private void runTest_getTermFrequency(
      final AbstractIndexDataProvider instance) {
    final Iterator<ByteArray> termsIt = this.referenceIndex.getTermsIterator();
    ByteArray term;
    while (termsIt.hasNext()) {
      term = termsIt.next();
      final long result = instance.getTermFrequencyIgnoringStopwords(term);
      // stopwords should be included.
      Assert.assertFalse(
          msg(instance, "Term frequency was zero. term=" + ByteArrayUtils.
              utf8ToString(term)),
          result <= 0L
      );
    }
  }

  /**
   * Test for getTermFrequencyIgnoringStopwords method, of class
   * AbstractIndexDataProvider. Using stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void test_getTermFrequency__stopped()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.STOPPED)) {
      runTest_getTermFrequency(instance);
    }
  }

  /**
   * Test for getTermFrequencyIgnoringStopwords method, of class
   * AbstractIndexDataProvider. Using random fields.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void test_getTermFrequency__randField()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.RANDFIELD)) {
      runTest_getTermFrequency(instance);
    }
  }

  /**
   * Test for getTermFrequencyIgnoringStopwords method, of class
   * AbstractIndexDataProvider. Using random fields & stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void test_getTermFrequency__randField_stopped()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTest_getTermFrequency(instance);
    }
  }

  /**
   * Test of getTerms method, of class AbstractIndexDataProvider. Testing
   * against {@link FixedTestIndexDataProvider}.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTerms__fixedIdx()
      throws Exception {
    if (!canTestAgainstFixedInstance() || !canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider) setupFixedInstanceTest()) {
      final Collection<ByteArray> iTerms = instance.getIdxTerms();
      final Collection<ByteArray> eTerms = FIXED_INDEX.getTermSet();

      Assert.assertEquals(msg(instance, "Term list size differs."),
          (long) eTerms.size(), (long) iTerms.size());
      Assert.assertTrue(msg(instance, "Term list content differs."),
          iTerms.containsAll(eTerms));
    }
  }

  /**
   * Test of getTerms method, of class AbstractIndexDataProvider. Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTerms__plain()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.PLAIN)) {
      runTestGetTerms(instance);
    }
  }

  /**
   * Test method for getTerms method, of class AbstractIndexDataProvider.
   *
   * @param instance Prepared instance to test
   */
  private void runTestGetTerms(final AbstractIndexDataProvider instance) {
    final Collection<ByteArray> iTerms = instance.getIdxTerms();
    final Collection<ByteArray> eTerms = this.referenceIndex.getTermSet();

    Assert.assertEquals(msg(instance, "Term list size differs."),
        (long) eTerms.size(), (long) iTerms.size());
    Assert.assertTrue(msg(instance, "Term list content differs."),
        iTerms.containsAll(eTerms));
  }

  /**
   * Test of getTerms method, of class AbstractIndexDataProvider. Using
   * stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTerms__stopped()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.STOPPED)) {
      runTestGetTerms(instance);
    }
  }

  /**
   * Test of getTerms method, of class AbstractIndexDataProvider. Using random
   * fields.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTerms__randField()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.RANDFIELD)) {
      runTestGetTerms(instance);
    }
  }

  /**
   * Test of getTerms method, of class AbstractIndexDataProvider. Using random
   * fields & stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTerms__randField_stopped()
      throws Exception {
    if (!canTestAbstractIdpMethods()) {
      return;
    }
    try (final AbstractIndexDataProvider instance =
             (AbstractIndexDataProvider)
                 setupInstanceForTesting(Setup.RANDFIELD_STOPPED)) {
      runTestGetTerms(instance);
    }
  }

  /**
   * Test setup types.
   */
  private enum Setup {
    /**
     * Plain index.
     */
    PLAIN,
    /**
     * Random document fields are chosen.
     */
    RANDFIELD,
    /**
     * Random stopwords are set.
     */
    STOPPED,
    /**
     * Random stopwords are set. Random document fields are chosen.
     */
    RANDFIELD_STOPPED
  }
}
