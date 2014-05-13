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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Shared test functions for {@link IndexDataProvider} implementing classes.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("checkstyle:methodname")
public abstract class IndexDataProviderTestCase
    extends TestCase {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      IndexDataProviderTestCase.class);

  /**
   * Test-Index to check results against.
   */
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected final TestIndexDataProvider referenceIndex;

  /**
   * Private empty constructor for utility class.
   *
   * @param referenceIndex TestIndex to check against
   */
  public IndexDataProviderTestCase(final TestIndexDataProvider referenceIndex) {
    super();
    assertTrue("TestIndex is not initialized.", TestIndexDataProvider.
        isInitialized());
    this.referenceIndex = referenceIndex;
  }

  protected abstract Class<? extends IndexDataProvider> getInstanceClass();

  protected abstract IndexDataProvider createInstance(final Set<String>
      fields, final Set<String> stopwords)
      throws Exception;

  private IndexDataProvider setupInstanceForTesting(final boolean randFields,
      final boolean randStopwords)
      throws Exception {
    Set<String> fields = null;
    Set<String> stopwords = null;
    if (randFields) {
      fields = TestIndexDataProvider.util.getRandomFields();
    }
    if (randStopwords) {
      stopwords = TestIndexDataProvider.util.getRandomStopWords();
    }
    this.referenceIndex.prepareTestEnvironment(fields, stopwords);
    return createInstance(this.referenceIndex.getDocumentFields(),
        this.referenceIndex.getStopwords());
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
   * Test method for getTermFrequency method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   */
  private void _testGetTermFrequency_0args(final IndexDataProvider instance) {
    assertEquals(
        msg(instance, "Term frequency differs."),
        this.referenceIndex.getTermFrequency(),
        instance.getTermFrequency());
  }

  /**
   * Test method for getTermFrequency method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private void _testGetTermFrequency_ByteArray(final IndexDataProvider instance)
      throws Exception {
    final boolean excludeStopwords = TestIndexDataProvider.reference
        .hasStopwords();
    final Iterator<ByteArray> idxTermsIt = this.referenceIndex
        .getTermsIterator();

    while (idxTermsIt.hasNext()) {
      final ByteArray idxTerm = idxTermsIt.next();
      assertEquals(
          msg(instance, "Term frequency differs (stopped: "
              + excludeStopwords + "). term=" + idxTerm),
          this.referenceIndex.getTermFrequency(idxTerm),
          instance.getTermFrequency(idxTerm)
      );

      if (excludeStopwords) {
        final Collection<ByteArray> stopwords = TestIndexDataProvider.reference
            .getStopwords();
        assertFalse(
            msg(instance, "Stopword found in term list."),
            stopwords.contains(idxTerm));
      }
    }
  }

  /**
   * Test method for getRelativeTermFrequency method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private void _testGetRelativeTermFrequency(final IndexDataProvider instance)
      throws Exception {
    final boolean excludeStopwords = TestIndexDataProvider.reference
        .hasStopwords();
    final Iterator<ByteArray> idxTermsIt = this.referenceIndex
        .getTermsIterator();

    while (idxTermsIt.hasNext()) {
      final ByteArray idxTerm = idxTermsIt.next();
      assertEquals(
          msg(instance, "Relative term frequency differs (stopped: "
              + excludeStopwords + "). term=" + idxTerm),
          this.referenceIndex.getRelativeTermFrequency(idxTerm),
          instance.getRelativeTermFrequency(idxTerm),
          0
      );

      if (excludeStopwords) {
        final Collection<ByteArray> stopwords = TestIndexDataProvider
            .reference.getStopwords();
        assertFalse(
            msg(instance, "Stopword found in term list."),
            stopwords.contains(idxTerm));
      }
    }
  }

  /**
   * Test method for getTermsIterator method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private void _testGetTermsIterator(final IndexDataProvider instance)
      throws Exception {
    final boolean excludeStopwords = TestIndexDataProvider.reference
        .hasStopwords();
    final Iterator<ByteArray> result = instance.getTermsIterator();

    int iterations = 0;
    while (result.hasNext()) {
      iterations++;
      if (excludeStopwords) {
        final Collection<ByteArray> stopwords = TestIndexDataProvider.reference
            .getStopwords();
        assertFalse(
            msg(instance, "Found stopword."),
            stopwords.contains(result.next()));
      } else {
        result.next();
      }
    }

    assertEquals(
        msg(instance, "Not all terms found while iterating."),
        instance.getUniqueTermsCount(),
        iterations);
    assertEquals(
        msg(instance, "Different values for unique terms reported."),
        this.referenceIndex.getUniqueTermsCount(),
        instance.getUniqueTermsCount());
  }

  /**
   * Test of getDocumentCount method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   */
  private void _testGetDocumentCount(final IndexDataProvider instance) {
    assertEquals(
        msg(instance, "Different number of documents reported."),
        this.referenceIndex.getDocumentCount(),
        instance.getDocumentCount());
  }

  /**
   * Test method for getDocumentModel method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private void _testGetDocumentModel(final IndexDataProvider instance)
      throws Exception {
    final boolean excludeStopwords = TestIndexDataProvider.reference
        .hasStopwords();
    final Iterator<Integer> docIdIt = this.referenceIndex
        .getDocumentIdIterator();

    while (docIdIt.hasNext()) {
      final Integer docId = docIdIt.next();
      final DocumentModel iDocModel = instance.getDocumentModel(docId);
      final DocumentModel eDocModel = this.referenceIndex.getDocumentModel
          (docId);

      if (!eDocModel.equals(iDocModel)) {
        for (final Entry<ByteArray, Long> e : eDocModel.termFreqMap.entrySet
            ()) {
          LOG.debug("e: {}={}", ByteArrayUtils.utf8ToString(e.getKey()), e.
              getValue());
        }
        for (final Entry<ByteArray, Long> e : iDocModel.termFreqMap.entrySet
            ()) {
          LOG.debug("i: {}={}", ByteArrayUtils.utf8ToString(e.getKey()), e.
              getValue());
        }
        for (final Entry<ByteArray, Long> e : TestIndexDataProvider.reference
            .getDocumentTermFrequencyMap(docId).entrySet()) {
          LOG.debug("m: {}={}", ByteArrayUtils.utf8ToString(e.getKey()), e.
              getValue());
        }
      }

      assertEquals(
          msg(instance, "Equals failed (stopped: " + excludeStopwords + ") " +
              "for docId=" + docId),
          eDocModel, iDocModel
      );

      if (excludeStopwords) {
        final Collection<ByteArray> stopwords = TestIndexDataProvider
            .reference.getStopwords();
        for (final ByteArray term : iDocModel.termFreqMap.keySet()) {
          assertFalse(
              msg(instance, "Found stopword in model."),
              stopwords.contains(term));
        }
      }
    }
  }

  /**
   * Test of getDocumentIdIterator method. Plain.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   */
  private void _testGetDocumentIdIterator(final IndexDataProvider instance) {
    final long docCount = this.referenceIndex.getDocumentCount();
    long docCountIt = 0;
    final Iterator<Integer> result = instance.getDocumentIdIterator();

    while (result.hasNext()) {
      docCountIt++;
      result.next();
    }
    assertEquals(
        msg(instance, "Number of document ids differ."),
        docCount, docCountIt);
  }

  /**
   * Test of getDocumentIdSource method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   */
  private void _testGetDocumentIdSource(final IndexDataProvider instance) {
    final Processing p = new Processing();

    p.setSource(instance.getDocumentIdSource());

    assertEquals(
        msg(instance, "Not all items provided by source or processed by " +
            "target."),
        this.referenceIndex.getDocumentCount(),
        p.debugTestSource().longValue()
    );
  }

  /**
   * Test of getUniqueTermsCount method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   */
  private void _testGetUniqueTermsCount(final IndexDataProvider instance)
      throws Exception {
    assertEquals(
        msg(instance, "Unique term count values are different."),
        TestIndexDataProvider.reference.getTermSet().size(),
        instance.getUniqueTermsCount());
  }

  /**
   * Test method for documentContains method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private void _testDocumentContains(final IndexDataProvider instance)
      throws Exception {
    final boolean excludeStopwords = TestIndexDataProvider.reference
        .hasStopwords();
    final Iterator<Integer> docIdIt = instance.getDocumentIdIterator();

    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final DocumentModel docModel = instance.getDocumentModel(docId);
      for (final ByteArray byteArray : docModel.termFreqMap.keySet()) {
        assertTrue(
            msg(instance, "Document contains term mismatch (stopped: " +
                excludeStopwords + ")."),
            instance.documentContains(docId, byteArray)
        );
        if (excludeStopwords) {
          final Collection<ByteArray> stopwords = TestIndexDataProvider
              .reference.getStopwords();
          assertFalse(
              msg(instance, "Found stopword."),
              stopwords.contains(byteArray));
        }
      }
    }
  }

  /**
   * Test of getTermsSource method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   */
  private void _testGetTermsSource(final IndexDataProvider instance)
      throws Exception {
    final Processing p = new Processing();
    p.setSource(instance.getTermsSource());

    assertEquals(
        msg(instance, "Not all items provided by source or processed by " +
            "target."),
        new HashSet<>(TestIndexDataProvider.reference.getTermList()).size(),
        p.debugTestSource().longValue()
    );
  }

  /**
   * Test method for getDocumentsTermSet method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  private void _testGetDocumentsTermSet(final IndexDataProvider instance)
      throws Exception {
    final boolean excludeStopwords = TestIndexDataProvider.reference
        .hasStopwords();
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

    assertEquals(
        msg(instance, "Not the same amount of terms retrieved (stopped: " +
            excludeStopwords + ")."),
        expResult.size(), result.size()
    );
    assertTrue(
        msg(instance, "Not all terms retrieved (stopped: " + excludeStopwords
            + ")."),
        expResult.containsAll(result)
    );

    if (excludeStopwords) {
      final Collection<ByteArray> stopwords = TestIndexDataProvider.reference
          .getStopwords();
      for (final ByteArray term : result) {
        assertFalse(
            msg(instance, "Stopword found in term list."),
            stopwords.contains(term));
      }
    }
  }

  /**
   * Test method for getDocumentFrequency method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  private void _testGetDocumentFrequency(final IndexDataProvider instance)
      throws Exception {
    final boolean excludeStopwords = TestIndexDataProvider.reference
        .hasStopwords();

    for (final ByteArray term : TestIndexDataProvider.reference.getTermSet()) {
      assertEquals(
          msg(instance, "Document frequency mismatch (stopped: " +
              excludeStopwords + ") (" + ByteArrayUtils.utf8ToString(term) +
              ")."),
          this.referenceIndex.getDocumentFrequency(term),
          instance.getDocumentFrequency(term)
      );

      if (excludeStopwords) {
        final Collection<ByteArray> stopwords = TestIndexDataProvider
            .reference.getStopwords();
        assertFalse(
            msg(instance, "Found stopword in term list."),
            stopwords.contains(term));
      }
    }
  }

  /**
   * Test method for getFieldId method, of class AbstractIndexDataProvider.
   *
   * @param instance Prepared instance to test
   */
  private void _testGetFieldId(final AbstractIndexDataProvider instance) {
    for (final String fieldName : this.referenceIndex.getDocumentFields()) {
      final SerializableByte result = instance.getFieldId(fieldName);
      assertNotNull(
          msg(instance, "Field id was null."),
          result);
    }
  }

  /**
   * Test method for _getTermFrequency method,
   * of class AbstractIndexDataProvider.
   *
   * @param instance Prepared instance to test
   */
  private void _test_getTermFrequency(
      final AbstractIndexDataProvider instance) {
    final Iterator<ByteArray> termsIt = this.referenceIndex.getTermsIterator();
    ByteArray term;
    while (termsIt.hasNext()) {
      term = termsIt.next();
      final long result = instance._getTermFrequency(term);
      // stopwords should be included.
      assertFalse(
          msg(instance, "Term frequency was zero. term=" + ByteArrayUtils.
              utf8ToString(term)),
          result <= 0L
      );
    }
  }

  /**
   * Test method for getTerms method, of class AbstractIndexDataProvider.
   *
   * @param instance Prepared instance to test
   */
  private void _testGetTerms(final AbstractIndexDataProvider instance) {
    final Collection<ByteArray> iTerms = instance.getIdxTerms();
    final Collection<ByteArray> eTerms = TestIndexDataProvider.reference
        .getTermSet();

    assertEquals(
        msg(instance, "Term list size differs."),
        eTerms.size(), iTerms.size());
    assertTrue(
        msg(instance, "Term list content differs."),
        iTerms.containsAll(eTerms));
  }

  /**
   * Check, if the current {@link IndexDataProvider} is implementing {@link
   * AbstractIndexDataProvider} class.
   *
   * @return True, if instance is implementing the abstract class
   */
  private boolean isImplementingAbstractIdp() {
    // skip test, if not implementing AbstractIndexDataProvider
    final Class<? extends IndexDataProvider> instance = getInstanceClass();
    if (!AbstractIndexDataProvider.class.isAssignableFrom(instance)) {
      LOG.warn(msg(instance, "Skipping test. "
          + "No sub-class of AbstractIndexDataProvider."));
      return false;
    }
    return true;
  }

  /**
   * Check, if the current testes {@link IndexDataProvider} is extending the
   * {@link de.unihildesheim.iw.lucene.index.AbstractIndexDataProvider}
   *
   * @return True, if it is extending
   */
  private boolean isAbstractIdpTestInstance() {
    // skip test for plain testing instance
    final Class<? extends IndexDataProvider> instance = getInstanceClass();
    if (instance.equals(AbstractIndexDataProviderTestImpl.class)) {
      LOG.warn(msg(instance, "Skipping test. Plain instance."));
      return true;
    }
    return false;
  }

  /**
   * Test method for getTermFrequency method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    _testGetTermFrequency_0args(setupInstanceForTesting(false, false));
  }

  /**
   * Test method for getTermFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }

    final long unfilteredTf = this.referenceIndex.getTermFrequency();
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testGetTermFrequency_0args(instance);

    // check with stopwords
    final long filteredTf = this.referenceIndex.getTermFrequency();
    assertEquals(
        msg(instance, "Term frequency differs. plain=" + unfilteredTf
            + " filter=" + filteredTf + "."),
        this.referenceIndex.getTermFrequency(),
        instance.getTermFrequency()
    );

    assertNotEquals(
        msg(instance,
            "TF using stop-words should be lower than without. filter="
                + filteredTf + " plain=" + unfilteredTf + "."
        ),
        filteredTf, unfilteredTf
    );
  }

  /**
   * Test method for getTermFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__randField()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }

    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetTermFrequency_0args(instance);
  }

  /**
   * Test method for getTermFrequency method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__randField_stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }

    final IndexDataProvider instance = setupInstanceForTesting(true, true);
    _testGetTermFrequency_0args(instance);
  }

  /**
   * Test of getTermFrequency method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_ByteArray__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    _testGetTermFrequency_ByteArray(setupInstanceForTesting(false, false));
  }

  /**
   * Test of getTermFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_ByteArray__stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testGetTermFrequency_ByteArray(instance);
  }

  /**
   * Test of getTermFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_ByteArray__randField()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetTermFrequency_ByteArray(instance);
  }

  /**
   * Test of getTermFrequency method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_ByteArray__randField_stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, true);
    _testGetTermFrequency_ByteArray(instance);
  }

  /**
   * Test of getRelativeTermFrequency method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    _testGetRelativeTermFrequency(setupInstanceForTesting(false, false));
  }

  /**
   * Test of getRelativeTermFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testGetRelativeTermFrequency(instance);
  }

  /**
   * Test of getRelativeTermFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__randField()
      throws
      Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetRelativeTermFrequency(instance);
  }

  /**
   * Test of getRelativeTermFrequency method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__randField_stopped()
      throws
      Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, true);
    _testGetRelativeTermFrequency(instance);
  }

  /**
   * Test of getTermsIterator method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsIterator__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, false);
    _testGetTermsIterator(instance);
  }

  /**
   * Test of getTermsIterator method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsIterator__stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testGetTermsIterator(instance);
  }

  /**
   * Test of getTermsIterator method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsIterator__randField()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetTermsIterator(instance);
  }

  /**
   * Test of getTermsIterator method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsIterator__randField_stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, true);
    _testGetTermsIterator(instance);
  }

  /**
   * Test of getDocumentCount method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, false);
    _testGetDocumentCount(instance);
  }

  /**
   * Test of getDocumentCount method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__randField()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetDocumentCount(instance);
  }

  /**
   * Test of getDocumentCount method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testGetDocumentCount(instance);
  }

  /**
   * Test of getDocumentCount method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__randField_stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, true);
    _testGetDocumentCount(instance);
  }

  /**
   * Test of getDocumentModel method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  public final void testGetDocumentModel__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, false);
    _testGetDocumentModel(instance);
  }

  /**
   * Test of getDocumentModel method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentModel__stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testGetDocumentModel(instance);
  }

  /**
   * Test of getDocumentModel method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  public final void testGetDocumentModel__randField()
      throws Exception {
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetDocumentModel(instance);
  }

  /**
   * Test of getDocumentModel method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  public final void testGetDocumentModel__randField_stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, true);
    _testGetDocumentModel(instance);
  }

  /**
   * Test of getDocumentIdIterator method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, false);
    _testGetDocumentIdIterator(instance);
  }

  /**
   * Test of getDocumentIdIterator method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator_stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testGetDocumentIdIterator(instance);
  }

  /**
   * Test of getDocumentIdIterator method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator_randField()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetDocumentIdIterator(instance);
  }

  /**
   * Test of getDocumentIdIterator method. Using random fields and stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator_randField_stopped()
      throws
      Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetDocumentIdIterator(instance);
  }

  /**
   * Test of getDocumentIdSource method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, false);
    _testGetDocumentIdSource(instance);
  }

  /**
   * Test of getDocumentIdSource method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource_stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testGetDocumentIdSource(instance);
  }

  /**
   * Test of getDocumentIdSource method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource_randField()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetDocumentIdSource(instance);
  }

  /**
   * Test of getDocumentIdSource method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource_randField_stopped()
      throws
      Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, true);
    _testGetDocumentIdSource(instance);
  }

  /**
   * Test of getUniqueTermsCount method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, false);
    _testGetUniqueTermsCount(instance);
  }

  /**
   * Test of getUniqueTermsCount method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testGetUniqueTermsCount(instance);
  }

  /**
   * Test of getUniqueTermsCount method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__randField()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetUniqueTermsCount(instance);
  }

  /**
   * Test of getUniqueTermsCount method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__randField_stopped()
      throws
      Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, true);
    _testGetUniqueTermsCount(instance);
  }

  /**
   * Test of hasDocument method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testHasDocument__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }

    final Iterator<Integer> docIdIt = this.referenceIndex
        .getDocumentIdIterator();
    final IndexDataProvider instance = setupInstanceForTesting(false, false);
    while (docIdIt.hasNext()) {
      assertTrue(
          msg(instance, "Document not found."),
          instance.hasDocument(docIdIt.next()));
    }

    assertFalse(
        msg(instance, "Document should not be found."),
        instance.hasDocument(-1));
    assertFalse(
        msg(instance, "Document should not be found."),
        instance.hasDocument((int) this.referenceIndex.getDocumentCount()));
  }

  /**
   * Test of documentContains method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDocumentContains__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, false);
    _testDocumentContains(instance);
  }

  /**
   * Test of documentContains method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDocumentContains__stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testDocumentContains(instance);
  }

  /**
   * Test of documentContains method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDocumentContains__randField()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testDocumentContains(instance);
  }

  /**
   * Test of documentContains method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDocumentContains__randField_stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, true);
    _testDocumentContains(instance);
  }

  /**
   * Test of getTermsSource method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, false);
    _testGetTermsSource(instance);
  }

  /**
   * Test of getTermsSource method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testGetTermsSource(instance);
  }

  /**
   * Test of getTermsSource method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__randField()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetTermsSource(instance);
  }

  /**
   * Test of getTermsSource method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__randField_stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, true);
    _testGetTermsSource(instance);
  }

  /**
   * Test of getDocumentsTermSet method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentsTermSet__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, false);
    _testGetDocumentsTermSet(instance);
  }

  /**
   * Test of getDocumentsTermSet method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentsTermSet__stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testGetDocumentsTermSet(instance);
  }

  /**
   * Test of getDocumentsTermSet method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentsTermSet__randField()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetDocumentsTermSet(instance);
  }

  /**
   * Test of getDocumentsTermSet method. Using stopwords & random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentsTermSet__randField_stopped()
      throws
      Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, true);
    _testGetDocumentsTermSet(instance);
  }

  /**
   * Test of getDocumentFrequency method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__plain()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, false);
    _testGetDocumentFrequency(instance);
  }

  /**
   * Test of getDocumentFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__stopped()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(false, true);
    _testGetDocumentFrequency(instance);
  }

  /**
   * Test of getDocumentFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__randField()
      throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, false);
    _testGetDocumentFrequency(instance);
  }

  /**
   * Test of getDocumentFrequency method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__randField_stopped()
      throws
      Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = setupInstanceForTesting(true, true);
    _testGetDocumentFrequency(instance);
  }

  /**
   * Test of warmUp method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUp__plain()
      throws Exception {
    setupInstanceForTesting(false, false).warmUp();
  }

  /**
   * Test of dispose method.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDispose__plain()
      throws Exception {
    setupInstanceForTesting(false, false).dispose();
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
    if (!isImplementingAbstractIdp()) {
      return;
    }

    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(false, false);
    instance.warmUpDocumentFrequencies();
  }

  /**
   * Test of getDocumentIds method (from {@link AbstractIndexDataProvider}).
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIds__plain()
      throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }

    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(false, false);
    final Collection<Integer> docIds = new ArrayList<>(instance.
        getDocumentIds());
    final Iterator<Integer> docIdIt =
        this.referenceIndex.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final Integer docId = docIdIt.next();
      assertTrue(
          msg(instance, "Doc-id was missing. docId=" + docId),
          docIds.remove(docId));
    }
    assertTrue(
        msg(instance, "Too much document ids provided by instance."),
        docIds.isEmpty());
  }

  /**
   * Test of warmUpTerms method (from {@link AbstractIndexDataProvider}). Using
   * stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUpTerms__plain()
      throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(false, false);
    instance.warmUpTerms();
  }

  /**
   * Test of warmUpIndexTermFrequencies method (from {@link
   * AbstractIndexDataProvider}). Using stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  @SuppressWarnings("checkstyle:nodesignedforextension")
  public void testWarmUpIndexTermFrequencies()
      throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(false, true);
    instance.warmUpIndexTermFrequencies();
  }

  /**
   * Test of warmUpDocumentIds method (from {@link AbstractIndexDataProvider}).
   * Using stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUpDocumentIds__plain()
      throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(false, true);
    instance.warmUpDocumentIds();
  }

  /**
   * Test of getFieldId method, of class AbstractIndexDataProvider. Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetFieldId__plain()
      throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(false, false);
    _testGetFieldId(instance);
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
    if (!isImplementingAbstractIdp()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(true, false);
    _testGetFieldId(instance);
  }

  /**
   * Test for _getTermFrequency method, of class AbstractIndexDataProvider.
   * Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void test_getTermFrequency__plain()
      throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(true, false);
    _test_getTermFrequency(instance);
  }

  /**
   * Test for _getTermFrequency method, of class AbstractIndexDataProvider.
   * Using stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void test_getTermFrequency__stopped()
      throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(false, true);
    _test_getTermFrequency(instance);
  }

  /**
   * Test for _getTermFrequency method, of class AbstractIndexDataProvider.
   * Using random fields.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void test_getTermFrequency__randField()
      throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(true, false);
    _test_getTermFrequency(instance);
  }

  /**
   * Test for _getTermFrequency method, of class AbstractIndexDataProvider.
   * Using random fields & stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void test_getTermFrequency__randField_stopped()
      throws
      Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(true, true);
    _test_getTermFrequency(instance);
  }

  /**
   * Test of getPersistence method, of class AbstractIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetPersistence__plain()
      throws Exception {
    // NOP: no suitable test for plain instance
  }

  /**
   * Test of getTerms method, of class AbstractIndexDataProvider. Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTerms__plain()
      throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(false, false);
    _testGetTerms(instance);
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
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(false, true);
    _testGetTerms(instance);
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
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(true, false);
    _testGetTerms(instance);
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
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(true, true);
    _testGetTerms(instance);
  }

  /**
   * Test of clearCache method, of class AbstractIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public final void testClearCache__plain()
      throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    final AbstractIndexDataProvider instance = (AbstractIndexDataProvider)
        setupInstanceForTesting(false, false);
    instance.clearCache();
    assertTrue(
        msg(instance, "Index terms cache not empty."),
        instance.getIdxTerms().isEmpty());
    assertTrue(
        msg(instance, "Index document frequency cache not empty."),
        instance.getIdxDfMap().isEmpty());
    assertNull(msg(instance, "Index term frequency cache not empty."),
        instance.getIdxTf());
  }
}
