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
package de.unihildesheim.lucene.index;

import de.unihildesheim.ByteArray;
import de.unihildesheim.SerializableByte;
import de.unihildesheim.TestMethodInfo;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.index.AbstractIndexDataProviderTest.AbstractIndexDataProviderTestImpl;
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.concurrent.processing.Processing;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared test functions for {@link IndexDataProvider} implementing classes.
 *
 * @author Jens Bertram
 */
public abstract class IndexDataProviderTestMethods {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          IndexDataProviderTestMethods.class);

  /**
   * Test-Index to check results against.
   */
  protected final TestIndexDataProvider index;
  /**
   * Current {@link IndexDataProvider} class to test.
   */
  protected final Class<? extends IndexDataProvider> dataProv;

  /**
   * Log test methods.
   */
  @Rule
  public final TestMethodInfo watcher = new TestMethodInfo();

  /**
   * Private empty constructor for utility class.
   *
   * @param newIndex TestIndex to check against
   * @param newDataProv DataProvider to test
   */
  public IndexDataProviderTestMethods(
          final TestIndexDataProvider newIndex,
          final Class<? extends IndexDataProvider> newDataProv) {
    assertTrue("TestIndex is not initialized.", TestIndexDataProvider.
            isInitialized());
    this.index = newIndex;
    this.dataProv = newDataProv;
  }

  private boolean isImplementingAbstractIdp() {
    // skip test, if not implementing AbstractIndexDataProvider
    if (!AbstractIndexDataProvider.class.isAssignableFrom(dataProv)) {
      LOG.warn("Skip test for " + AbstractIndexDataProviderTestImpl.class.
              getCanonicalName()
              + ". No sub-class of AbstractIndexDataProvider.");
      return false;
    }
    return true;
  }

  private boolean isAbstractIdpTestInstance() {
    // skip test for plain testing instance
    if (dataProv.equals(AbstractIndexDataProviderTestImpl.class)) {
      LOG.warn("Skip test for " + AbstractIndexDataProviderTestImpl.class.
              getCanonicalName());
      return true;
    }
    return false;
  }

  /**
   * Run after each test has finished.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @After
  public void tearDown() throws Exception {
    if (Environment.isInitialized()) {
      Environment.getDataProvider().dispose();
    }
    Environment.clear();
    Environment.clearAllProperties();
  }

  /**
   * Test method for getTermFrequency method.
   *
   * @param index Test index to get base data from
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  protected static void _testGetTermFrequency_0args(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    assertEquals("Term frequency differs.", index.getTermFrequency(),
            instance.getTermFrequency());
  }

  /**
   * Test method for getTermFrequency method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv, null, null);
    _testGetTermFrequency_0args(index, instance);
  }

  /**
   * Test method for getTermFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }

    final long unfilteredTf = index.getTermFrequency();
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testGetTermFrequency_0args(index, instance);

    // check with stopwords
    final long filteredTf = index.getTermFrequency();
    assertEquals("Term frequency differs. plain=" + unfilteredTf + " filter="
            + filteredTf + ".", index.getTermFrequency(),
            instance.getTermFrequency());

    assertNotEquals(
            "TF using stop-words should be lower than without. filter="
            + filteredTf + " plain=" + unfilteredTf + ".",
            filteredTf, unfilteredTf);
  }

  /**
   * Test method for getTermFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__randField() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }

    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetTermFrequency_0args(index, instance);
  }

  /**
   * Test method for getTermFrequency method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_0args__randField_stopped() throws
          Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }

    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index),
            IndexTestUtil.getRandomStopWords(index));
    _testGetTermFrequency_0args(index, instance);
  }

  /**
   * Test method for getTermFrequency method.
   *
   * @param index Test index to get base data from
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testGetTermFrequency_ByteArray(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = !(stopwords == null || stopwords.
            isEmpty());

    final Iterator<ByteArray> idxTermsIt = index.getTermsIterator();
    while (idxTermsIt.hasNext()) {
      final ByteArray idxTerm = idxTermsIt.next();
      assertEquals("Term frequency differs (stopped: " + excludeStopwords
              + "). term=" + idxTerm, index.getTermFrequency(idxTerm),
              instance.getTermFrequency(idxTerm));
      if (excludeStopwords) {
        assertFalse("Stopword found in term list.", stopwords.contains(
                idxTerm));
      }
    }
  }

  /**
   * Test of getTermFrequency method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_ByteArray__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    _testGetTermFrequency_ByteArray(index, IndexTestUtil.createInstance(index,
            dataProv, null, null));
  }

  /**
   * Test of getTermFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_ByteArray__stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testGetTermFrequency_ByteArray(index, instance);
  }

  /**
   * Test of getTermFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermFrequency_ByteArray__randField() throws
          Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetTermFrequency_ByteArray(index, instance);
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
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index),
            IndexTestUtil.getRandomStopWords(index));
    _testGetTermFrequency_ByteArray(index, instance);
  }

  /**
   * Test method for getRelativeTermFrequency method.
   *
   * @param index Test index to get base data from
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testGetRelativeTermFrequency(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    Iterator<ByteArray> idxTermsIt = index.getTermsIterator();

    while (idxTermsIt.hasNext()) {
      final ByteArray idxTerm = idxTermsIt.next();
      assertEquals("Relative term frequency differs (stopped: "
              + excludeStopwords + "). term=" + idxTerm, index.
              getRelativeTermFrequency(idxTerm), instance.
              getRelativeTermFrequency(idxTerm), 0);
      if (excludeStopwords) {
        assertFalse("Stopword found in term list.", stopwords.contains(
                idxTerm));
      }
    }
  }

  /**
   * Test of getRelativeTermFrequency method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    _testGetRelativeTermFrequency(index, IndexTestUtil.createInstance(index,
            dataProv, null, null));
  }

  /**
   * Test of getRelativeTermFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testGetRelativeTermFrequency(index, instance);
  }

  /**
   * Test of getRelativeTermFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__randField() throws
          Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetRelativeTermFrequency(index, instance);
  }

  /**
   * Test of getRelativeTermFrequency method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetRelativeTermFrequency__randField_stopped() throws
          Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index),
            IndexTestUtil.getRandomStopWords(index));
    _testGetRelativeTermFrequency(index, instance);
  }

  /**
   * Test method for getTermsIterator method.
   *
   * @param index Test index to get base data from
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testGetTermsIterator(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    Iterator<ByteArray> result = instance.getTermsIterator();

    int iterations = 0;
    while (result.hasNext()) {
      iterations++;
      if (excludeStopwords) {
        assertFalse("Found stopword.", stopwords.contains(result.next()));
      } else {
        result.next();
      }
    }

    assertEquals("Not all terms found while iterating.", instance.
            getUniqueTermsCount(), iterations);
    assertEquals("Different values for unique terms reported.", index.
            getUniqueTermsCount(), instance.getUniqueTermsCount());
  }

  /**
   * Test of getTermsIterator method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("DM_DEFAULT_ENCODING")
  public final void testGetTermsIterator__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, null);
    _testGetTermsIterator(index, instance);
  }

  /**
   * Test of getTermsIterator method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("DM_DEFAULT_ENCODING")
  public final void testGetTermsIterator__stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testGetTermsIterator(index, instance);
  }

  /**
   * Test of getTermsIterator method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("DM_DEFAULT_ENCODING")
  public final void testGetTermsIterator__randField() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetTermsIterator(index, instance);
  }

  /**
   * Test of getTermsIterator method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("DM_DEFAULT_ENCODING")
  public final void testGetTermsIterator__randField_stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index),
            IndexTestUtil.getRandomStopWords(index));
    _testGetTermsIterator(index, instance);
  }

  /**
   * Test of getDocumentCount method.
   *
   * @param index Test index to get base data from
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  private static void _testGetDocumentCount(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final long expResult = index.getDocumentCount();
    final long result = instance.getDocumentCount();
    assertEquals("Different number of documents reported.", expResult, result);
  }

  /**
   * Test of getDocumentCount method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, null);
    _testGetDocumentCount(index, instance);
  }

  /**
   * Test of getDocumentCount method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__randField() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetDocumentCount(index, instance);
  }

  /**
   * Test of getDocumentCount method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testGetDocumentCount(index, instance);
  }

  /**
   * Test of getDocumentCount method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentCount__randField_stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index),
            IndexTestUtil.getRandomStopWords(index));
    _testGetDocumentCount(index, instance);
  }

  /**
   * Test method for getDocumentModel method, of class
   * CachedIndexDataProvider.
   *
   * @param index Test index to get base data from
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testGetDocumentModel(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final Integer docId = docIdIt.next();
      final DocumentModel iDocModel = instance.getDocumentModel(docId);
      final DocumentModel eDocModel = index.getDocumentModel(docId);

      if (!eDocModel.equals(iDocModel)) {
        for (Entry<ByteArray, Long> e : eDocModel.termFreqMap.entrySet()) {
          LOG.debug("e: {}={}", ByteArrayUtil.utf8ToString(e.getKey()), e.
                  getValue());
        }
        for (Entry<ByteArray, Long> e : iDocModel.termFreqMap.entrySet()) {
          LOG.debug("i: {}={}", ByteArrayUtil.utf8ToString(e.getKey()), e.
                  getValue());
        }
        for (Entry<ByteArray, Long> e : index.getDocumentTermFrequencyMap(
                docId).entrySet()) {
          LOG.debug("m: {}={}", ByteArrayUtil.utf8ToString(e.getKey()), e.
                  getValue());
        }
      }

      assertTrue("Equals failed (stopped: " + excludeStopwords
              + ") for docId=" + docId, eDocModel.equals(iDocModel));

      if (excludeStopwords) {
        for (ByteArray term : iDocModel.termFreqMap.keySet()) {
          assertFalse("Found stopword in model.", stopwords.contains(term));
        }
      }
    }
  }

  /**
   * Test of getDocumentModel method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  public final void testGetDocumentModel__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, null);
    _testGetDocumentModel(index, instance);
  }

  /**
   * Test of getDocumentModel method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentModel__stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testGetDocumentModel(index, instance);
  }

  /**
   * Test of getDocumentModel method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  public final void testGetDocumentModel__randField() throws Exception {
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetDocumentModel(index, instance);
  }

  /**
   * Test of getDocumentModel method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  public final void testGetDocumentModel__randField_stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index),
            IndexTestUtil.getRandomStopWords(index));
    _testGetDocumentModel(index, instance);
  }

  /**
   * Test of getDocumentIdIterator method. Plain.
   *
   * @param index Test index to get base data from
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  private static void _testGetDocumentIdIterator(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final long docCount = index.getDocumentCount();
    long docCountIt = 0;
    final Iterator<Integer> result = instance.getDocumentIdIterator();
    while (result.hasNext()) {
      docCountIt++;
      result.next();
    }
    assertEquals(docCount, docCountIt);
  }

  /**
   * Test of getDocumentIdIterator method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv, null, null);
    _testGetDocumentIdIterator(index, instance);
  }

  /**
   * Test of getDocumentIdIterator method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator_stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testGetDocumentIdIterator(index, instance);
  }

  /**
   * Test of getDocumentIdIterator method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator_randField() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetDocumentIdIterator(index, instance);
  }

  /**
   * Test of getDocumentIdIterator method. Using random fields and stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdIterator_randField_stopped() throws
          Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetDocumentIdIterator(index, instance);
  }

  /**
   * Test of getDocumentIdSource method.
   *
   * @param index Test index to get base data from
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("UnnecessaryUnboxing")
  private static void _testGetDocumentIdSource(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final long docCount = index.getDocumentCount();
    Processing p = new Processing();
    p.setSource(instance.getDocumentIdSource());
    assertEquals("Not all items provided by source or processed by target.",
            docCount, p.debugTestSource().longValue());
  }

  /**
   * Test of getDocumentIdSource method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv, null, null);
    _testGetDocumentIdSource(index, instance);
  }

  /**
   * Test of getDocumentIdSource method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource_stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testGetDocumentIdSource(index, instance);
  }

  /**
   * Test of getDocumentIdSource method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource_randField() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetDocumentIdSource(index, instance);
  }

  /**
   * Test of getDocumentIdSource method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIdSource_randField_stopped() throws
          Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index),
            IndexTestUtil.getRandomStopWords(index));
    _testGetDocumentIdSource(index, instance);
  }

  /**
   * Test of getUniqueTermsCount method.
   *
   * @param index Test index to get base data from
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  private static void _testGetUniqueTermsCount(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    assertEquals("Unique term count values are different.",
            index.getTermSet().size(), instance.getUniqueTermsCount());
  }

  /**
   * Test of getUniqueTermsCount method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv, null, null);
    _testGetUniqueTermsCount(index, instance);
  }

  /**
   * Test of getUniqueTermsCount method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testGetUniqueTermsCount(index, instance);
  }

  /**
   * Test of getUniqueTermsCount method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__randField() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetUniqueTermsCount(index, instance);
  }

  /**
   * Test of getUniqueTermsCount method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetUniqueTermsCount__randField_stopped() throws
          Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), IndexTestUtil.
            getRandomStopWords(index));
    _testGetUniqueTermsCount(index, instance);
  }

  /**
   * Test of hasDocument method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testHasDocument__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }

    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, null);
    while (docIdIt.hasNext()) {
      assertTrue("Document not found.", instance.hasDocument(docIdIt.next()));
    }

    assertFalse("Document should not be found.", instance.hasDocument(-1));
    assertFalse("Document should not be found.", instance.hasDocument(
            (int) index.getDocumentCount()));
  }

  /**
   * Test method for documentContains method.
   *
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testDocumentContains(final IndexDataProvider instance)
          throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    Iterator<Integer> docIdIt = instance.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final int docId = docIdIt.next();
      final DocumentModel docModel = instance.getDocumentModel(docId);
      for (ByteArray byteArray : docModel.termFreqMap.keySet()) {
        assertTrue("Document contains term mismatch (stopped: "
                + excludeStopwords + ").", instance.documentContains(
                        docId, byteArray));
        if (excludeStopwords) {
          assertFalse("Found stopword.", stopwords.contains(byteArray));
        }
      }
    }
  }

  /**
   * Test of documentContains method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDocumentContains__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv, null, null);
    _testDocumentContains(instance);
  }

  /**
   * Test of documentContains method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDocumentContains__stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testDocumentContains(instance);
  }

  /**
   * Test of documentContains method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDocumentContains__randField() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testDocumentContains(instance);
  }

  /**
   * Test of documentContains method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDocumentContains__randField_stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index),
            IndexTestUtil.getRandomStopWords(index));
    _testDocumentContains(instance);
  }

  /**
   * Test of getTermsSource method.
   *
   * @param index Test index to get base data from
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("UnnecessaryUnboxing")
  private static void _testGetTermsSource(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final int termsCount = new HashSet<>(index.getTermList()).size();
    Processing p = new Processing();
    p.setSource(instance.getTermsSource());
    assertEquals("Not all items provided by source or processed by target.",
            termsCount, p.debugTestSource().longValue());
  }

  /**
   * Test of getTermsSource method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv, null, null);
    _testGetTermsSource(index, instance);
  }

  /**
   * Test of getTermsSource method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testGetTermsSource(index, instance);
  }

  /**
   * Test of getTermsSource method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__randField() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetTermsSource(index, instance);
  }

  /**
   * Test of getTermsSource method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetTermsSource__randField_stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index),
            IndexTestUtil.getRandomStopWords(index));
    _testGetTermsSource(index, instance);
  }

  /**
   * Test method for getDocumentsTermSet method.
   *
   * @param index Test index to get base data from
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testGetDocumentsTermSet(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    final int docAmount = RandomValue.getInteger(2, (int) index.
            getDocumentCount() - 1);
    Collection<Integer> docIds = new HashSet<>(docAmount);
    for (int i = 0; i < docAmount;) {
      if (docIds.add(RandomValue.getInteger(0, RandomValue.getInteger(2,
              (int) index.getDocumentCount() - 1)))) {
        i++;
      }
    }
    Collection<ByteArray> expResult = index.getDocumentsTermSet(docIds);
    Collection<ByteArray> result = instance.getDocumentsTermSet(docIds);

    assertEquals("Not the same amount of terms retrieved (stopped: "
            + excludeStopwords + ").", expResult.size(),
            result.size());
    assertTrue("Not all terms retrieved (stopped: "
            + excludeStopwords + ").", expResult.containsAll(result));

    if (excludeStopwords) {
      for (ByteArray term : result) {
        assertFalse("Stopword found in term list.", stopwords.contains(term));
      }
    }
  }

  /**
   * Test of getDocumentsTermSet method. Plain.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentsTermSet__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv, null, null);
    _testGetDocumentsTermSet(index, instance);
  }

  /**
   * Test of getDocumentsTermSet method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentsTermSet__stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testGetDocumentsTermSet(index, instance);
  }

  /**
   * Test of getDocumentsTermSet method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentsTermSet__randField() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetDocumentsTermSet(index, instance);
  }

  /**
   * Test of getDocumentsTermSet method. Using stopwords & random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentsTermSet__randField_stopped() throws
          Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index),
            IndexTestUtil.getRandomStopWords(index));
    _testGetDocumentsTermSet(index, instance);
  }

  /**
   * Test method for getDocumentFrequency method.
   *
   * @param index Test index to get base data from
   * @param instance {@link IndexDataProvider} implementation to test
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("null")
  private static void _testGetDocumentFrequency(
          final TestIndexDataProvider index,
          final IndexDataProvider instance) throws Exception {
    final Collection<ByteArray> stopwords = IndexTestUtil.
            getStopwordBytesFromEnvironment();
    final boolean excludeStopwords = stopwords != null;

    for (ByteArray term : index.getTermSet()) {
      assertEquals("Document frequency mismatch (stopped: "
              + excludeStopwords + ") (" + ByteArrayUtil.
              utf8ToString(term) + ").", index.getDocumentFrequency(term),
              instance.getDocumentFrequency(term));
      if (excludeStopwords) {
        assertFalse("Found stopword in term list.", stopwords.contains(term));
      }
    }
  }

  /**
   * Test of getDocumentFrequency method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__plain() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv, null, null);
    _testGetDocumentFrequency(index, instance);
  }

  /**
   * Test of getDocumentFrequency method. Using stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__stopped() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            null, IndexTestUtil.getRandomStopWords(index));
    _testGetDocumentFrequency(index, instance);
  }

  /**
   * Test of getDocumentFrequency method. Using random fields.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__randField() throws Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index), null);
    _testGetDocumentFrequency(index, instance);
  }

  /**
   * Test of getDocumentFrequency method. Using random fields & stopwords.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentFrequency__randField_stopped() throws
          Exception {
    if (isAbstractIdpTestInstance()) {
      return;
    }
    final IndexDataProvider instance = IndexTestUtil.createInstance(
            index, dataProv,
            IndexTestUtil.getRandomFields(index),
            IndexTestUtil.getRandomStopWords(index));
    _testGetDocumentFrequency(index, instance);
  }

  /**
   * Test of warmUp method.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUp__plain() throws Exception {
    IndexTestUtil.createInstance(index, dataProv, null, null).warmUp();
  }

  /**
   * Test of dispose method.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testDispose__plain() throws Exception {
    IndexTestUtil.createInstance(index, dataProv, null, null).dispose();
  }

  /**
   * Test of createCache method.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  public final void testCreateCache__plain() throws Exception {
    IndexTestUtil.createInstance(index, dataProv, null, null).createCache(
            RandomValue.getString(10));
  }

  /**
   * Test of loadOrCreateCache method.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  public final void testLoadOrCreateCache__plain() throws Exception {
    IndexTestUtil.createInstance(index, dataProv, null, null).
            loadOrCreateCache(RandomValue.getString(10));
  }

  /**
   * Test of loadCache method.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  public final void testLoadCache__plain() throws Exception {
    boolean thrown = false;
    try {
      IndexTestUtil.createInstance(index, dataProv, null, null).
              loadCache(RandomValue.getString(10));
    } catch (Exception ex) {
      thrown = true;
    }
    if (!thrown) {
      fail("Expected to catch an exception.");
    }
  }

  /**
   * Test of warmUpDocumentFrequencies method (from
   * {@link AbstractIndexDataProvider}).
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUpDocumentFrequencies__plain() throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }

    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(index, dataProv, null, null);
    instance.warmUpDocumentFrequencies();
  }

  /**
   * Test of getDocumentIds method (from {@link AbstractIndexDataProvider}).
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testGetDocumentIds__plain() throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }

    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(index, dataProv, null, null);
    final Collection<Integer> docIds = new ArrayList<>(instance.
            getDocumentIds());
    final Iterator<Integer> docIdIt = index.getDocumentIdIterator();
    while (docIdIt.hasNext()) {
      final Integer docId = docIdIt.next();
      assertTrue("Doc-id was missing. docId=" + docId, docIds.remove(docId));
    }
    assertTrue("Too much document ids provided by instance.", docIds.
            isEmpty());
  }

  /**
   * Test of testSetStopwordsFromEnvironment method (from
   * {@link AbstractIndexDataProvider}). Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testSetStopwordsFromEnvironment__plain() throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(index, dataProv, null, null);
    instance.setStopwordsFromEnvironment();
  }

  /**
   * Test of testSetStopwordsFromEnvironment method (from
   * {@link AbstractIndexDataProvider}). Using stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testSetStopwordsFromEnvironment__stopped() throws
          Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.createInstance(
                    index, dataProv,
                    null, IndexTestUtil.getRandomStopWords(index));
    instance.setStopwordsFromEnvironment();
  }

  /**
   * Test of warmUpTerms method (from {@link AbstractIndexDataProvider}).
   * Using stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUpTerms__plain() throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.createInstance(
                    index, dataProv, null, null);
    instance.warmUpTerms();
  }

  /**
   * Test of warmUpIndexTermFrequencies method (from
   * {@link AbstractIndexDataProvider}). Using stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testWarmUpIndexTermFrequencies() throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.createInstance(
                    index, dataProv,
                    null, IndexTestUtil.getRandomStopWords(index));
    instance.warmUpIndexTermFrequencies();
  }

  /**
   * Test of warmUpDocumentIds method (from
   * {@link AbstractIndexDataProvider}). Using stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void testWarmUpDocumentIds__plain() throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.createInstance(
                    index, dataProv,
                    null, IndexTestUtil.getRandomStopWords(index));
    instance.warmUpDocumentIds();
  }

  /**
   * Test method for getFieldId method, of class AbstractIndexDataProvider.
   *
   * @param instance Prepared instance to test
   * @throws Exception
   */
  private static void _testGetFieldId(
          final AbstractIndexDataProvider instance) {
    for (String fieldName : Environment.getFields()) {
      SerializableByte result = instance.getFieldId(fieldName);
      assertFalse("Field id was null.", result == null);
    }
  }

  /**
   * Test of getFieldId method, of class AbstractIndexDataProvider. Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetFieldId__plain() throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(index, dataProv, null, null);
    _testGetFieldId(instance);
  }

  /**
   * Test of getFieldId method, of class AbstractIndexDataProvider. Using
   * random fields.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetFieldId__randField() throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(
                    index, dataProv,
                    IndexTestUtil.getRandomFields(index), null);
    _testGetFieldId(instance);
  }

  /**
   * Test method for _getTermFrequency method, of class
   * AbstractIndexDataProvider.
   *
   * @param index Index to test against
   * @param instance Prepared instance to test
   */
  private static void _test_getTermFrequency(
          final TestIndexDataProvider index,
          final AbstractIndexDataProvider instance) {
    final Iterator<ByteArray> termsIt = index.getTermsIterator();
    ByteArray term;
    while (termsIt.hasNext()) {
      term = termsIt.next();
      long result = instance._getTermFrequency(term);
      // stopwords should be included.
      assertFalse("Term frequency was zero. term=" + ByteArrayUtil.
              utf8ToString(term), result <= 0L);
    }
  }

  /**
   * Test for _getTermFrequency method, of class AbstractIndexDataProvider.
   * Plain.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void test_getTermFrequency__plain() throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(
                    index, dataProv,
                    IndexTestUtil.getRandomFields(index), null);
    _test_getTermFrequency(index, instance);
  }

  /**
   * Test for _getTermFrequency method, of class AbstractIndexDataProvider.
   * Using stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void test_getTermFrequency__stopped() throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(
                    index, dataProv,
                    null, IndexTestUtil.getRandomStopWords(index));
    _test_getTermFrequency(index, instance);
  }

  /**
   * Test for _getTermFrequency method, of class AbstractIndexDataProvider.
   * Using random fields.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void test_getTermFrequency__randField() throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(
                    index, dataProv,
                    IndexTestUtil.getRandomFields(index), null);
    _test_getTermFrequency(index, instance);
  }

  /**
   * Test for _getTermFrequency method, of class AbstractIndexDataProvider.
   * Using random fields & stopwords.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public final void test_getTermFrequency__randField_stopped() throws
          Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(
                    index, dataProv,
                    IndexTestUtil.getRandomFields(index),
                    IndexTestUtil.getRandomStopWords(index));
    _test_getTermFrequency(index, instance);
  }

  /**
   * Test of getPersistence method, of class AbstractIndexDataProvider.
   *
   * @throws java.lang.Exception Any exception thrown indicates an error
   */
  @Test
  public void testGetPersistence__plain() throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    // NOP: no suitable test for plain instance
  }

  /**
   * Test method for getTerms method, of class AbstractIndexDataProvider.
   */
  private static void _testGetTerms(
          final TestIndexDataProvider index,
          final AbstractIndexDataProvider instance) {
    final Collection<ByteArray> iTerms = instance.idxTerms;
    final Collection<ByteArray> eTerms = index.getTermSet();

    assertEquals("Term list size differs.", eTerms.size(), iTerms.size());
    assertTrue("Term list content differs.", iTerms.containsAll(eTerms));
  }

  /**
   * Test of getTerms method, of class AbstractIndexDataProvider. Plain.
   */
  @Test
  public void testGetTerms__plain() throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(index, dataProv, null, null);
    _testGetTerms(index, instance);
  }

  /**
   * Test of getTerms method, of class AbstractIndexDataProvider. Using
   * stopwords.
   */
  @Test
  public void testGetTerms__stopped() throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(
                    index, dataProv,
                    null, IndexTestUtil.getRandomStopWords(index));
    _testGetTerms(index, instance);
  }

  /**
   * Test of getTerms method, of class AbstractIndexDataProvider. Using random
   * fields.
   */
  @Test
  public void testGetTerms__randField() throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(
                    index, dataProv,
                    IndexTestUtil.getRandomFields(index), null);
    _testGetTerms(index, instance);
  }

  /**
   * Test of getTerms method, of class AbstractIndexDataProvider. Using random
   * fields & stopwords.
   */
  @Test
  public void testGetTerms__randField_stopped() throws Exception {
    if (!isImplementingAbstractIdp() || isAbstractIdpTestInstance()) {
      return;
    }
    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(
                    index, dataProv,
                    IndexTestUtil.getRandomFields(index),
                    IndexTestUtil.getRandomStopWords(index));
    _testGetTerms(index, instance);
  }

  /**
   * Test of clearCache method, of class AbstractIndexDataProvider.
   *
   * @throws Exception Any exception indicates an error
   */
  @Test
  public void testClearCache__plain() throws Exception {
    if (!isImplementingAbstractIdp()) {
      return;
    }
    final AbstractIndexDataProvider instance
            = (AbstractIndexDataProvider) IndexTestUtil.
            createInstance(index, dataProv, null, null);
    instance.clearCache();
    assertTrue("Index terms cache not empty.", instance.idxTerms.isEmpty());
    assertTrue("Index document frequency cache not empty.", instance.idxDfMap.
            isEmpty());
    assertEquals("Index term frequency cache not empty.", null, instance.idxTf);
  }
}
