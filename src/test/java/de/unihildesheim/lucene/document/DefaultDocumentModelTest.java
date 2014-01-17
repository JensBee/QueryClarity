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
package de.unihildesheim.lucene.document;

import de.unihildesheim.lucene.TestUtility;
import de.unihildesheim.lucene.index.TestIndex;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class DefaultDocumentModelTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DefaultDocumentModelTest.class);

  /**
   * Default document id to set for testing models.
   */
  private static final int defaultDocId = 157;

  /**
   * Overall term frequency in the test model.
   */
  private static long termFreq;

  /**
   * Test terms with their frequencies to add to the model.
   */
  private static Map<String, Long> terms;

  public DefaultDocumentModelTest() {
  }

  @BeforeClass
  public static void setUpClass() {
    DefaultDocumentModelTest.terms = new HashMap(10);
    DefaultDocumentModelTest.terms.put("lucene", 5L);
    DefaultDocumentModelTest.terms.put("foo", 3L);
    DefaultDocumentModelTest.terms.put("bar", 15L);
    DefaultDocumentModelTest.terms.put("baz", 30L);
    DefaultDocumentModelTest.terms.put("model", 7L);
    DefaultDocumentModelTest.terms.put("found", 36L);
    DefaultDocumentModelTest.terms.put("property", 2L);
    DefaultDocumentModelTest.terms.put("coffee", 52L);
    DefaultDocumentModelTest.terms.put("java", 9L);
    DefaultDocumentModelTest.terms.put("overflow", 238L);

    // calculate overall term frequency
    termFreq = 0L;
    for (Long freq : DefaultDocumentModelTest.terms.values()) {
      termFreq += freq;
    }
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  /**
   * Create a new {@link DefaultDocumentModel} set-up for populating with the
   * default terms.
   *
   * @return Test {@link DefaultDocumentModel} instance
   */
  private DefaultDocumentModel createModelInstance() {
    return new DefaultDocumentModel(defaultDocId,
            DefaultDocumentModelTest.terms.size());
  }

  /**
   * Add all default terms to the given document model.
   *
   * @param docModel Model to add the terms to
   */
  private void addTermsToModel(DefaultDocumentModel docModel) {
    for (Entry<String, Long> entry : DefaultDocumentModelTest.terms.entrySet()) {
      docModel.setTermFrequency(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Get a random entry from the {@link DefaultDocumentModelTest#terms} map.
   *
   * @return Random {@link Entry} from the
   * {@link DefaultDocumentModelTest#terms} map
   */
  private Entry<String, Long> getRandomTermEntry() {
    final String[] docTerms = DefaultDocumentModelTest.terms.keySet().toArray(
            new String[DefaultDocumentModelTest.terms.size()]);
    final String term = docTerms[TestUtility.getRandInt(0, docTerms.length - 1)];

    return new AbstractMap.SimpleEntry<>(term, DefaultDocumentModelTest.terms.
            get(term));
  }

  /**
   * Test of setTermData method, of class DefaultDocumentModel.
   */
  @Test
  public void testSetTermData() {
    TestUtility.logHeader(LOG, "setTermData");

    final Entry<String, Long> entry = getRandomTermEntry();
    final String key = "test";
    final String value = "foo";

    final DefaultDocumentModel instance = createModelInstance();

    instance.setTermData(entry.getKey(), key, value);
  }

  /**
   * Test of getTermData (general object) method, of class DefaultDocumentModel.
   */
  @Test
  public void testGetTermDataObject() {
    TestUtility.logHeader(LOG, "getTermData - object");

    final Entry<String, Long> entry = getRandomTermEntry();
    final String key = "test";
    final String value = "foo";

    final DefaultDocumentModel instance = createModelInstance();

    instance.setTermData(entry.getKey(), key, value);
    final Object result = instance.getTermData(entry.getKey(), key);

    final Object expResult = value;

    assertEquals(expResult, result);
  }

  /**
   * Test of getTermData (typed) method, of class DefaultDocumentModel.
   */
  @Test
  public void testGetTermDataTyped() {
    TestUtility.logHeader(LOG, "getTermData - typed");

    final Entry<String, Long> entry = getRandomTermEntry();
    final String key = "test";
    final String value = "foo";

    final DefaultDocumentModel instance = createModelInstance();

    instance.setTermData(entry.getKey(), key, value);
    final String result = instance.
            getTermData(String.class, entry.getKey(), key);

    final Object expResult = value;

    assertEquals(expResult, result);
  }

  /**
   * Test of clearTermData method, of class DefaultDocumentModel.
   */
  @Test
  public void testClearTermData() {
    TestUtility.logHeader(LOG, "clearTermData");

    final String[] set1 = new String[]{"fooTerm", "delMeKey", "fooVal"};
    final String[] set2 = new String[]{"barTerm", "keepMeKey", "barVal"};
    final String[] set3 = new String[]{"bazTerm", "keepMeTooKey", "bazVal"};

    final DefaultDocumentModel instance = createModelInstance();

    instance.setTermData(set1[0], set1[1], set1[2]);
    instance.setTermData(set2[0], set2[1], set2[2]);
    instance.setTermData(set3[0], set3[1], set3[2]);

    LOG.info("Checking if all data is set.");
    assertEquals(set1[2], instance.getTermData(set1[0], set1[1]));
    assertEquals(set2[2], instance.getTermData(set2[0], set2[1]));
    assertEquals(set3[2], instance.getTermData(set3[0], set3[1]));
    // random check if other key is unset
    assertEquals(null, instance.getTermData(set1[0], set3[1]));

    LOG.info("Removing data.");
    instance.clearTermData(set1[1]);

    LOG.info("Check if data was removed.");
    assertEquals(null, instance.getTermData(set1[0], set1[1]));

    LOG.info("Check if other data is still there.");
    assertEquals(set2[2], instance.getTermData(set2[0], set2[1]));
    assertEquals(set3[2], instance.getTermData(set3[0], set3[1]));
  }

  /**
   * Test of containsTerm method, of class DefaultDocumentModel.
   */
  @Test
  public void testContainsTerm() {
    TestUtility.logHeader(LOG, "containsTerm");

    final Entry<String, Long> entry = getRandomTermEntry();

    final DefaultDocumentModel instance = createModelInstance();

    // no terms stored
    assertEquals(false, instance.containsTerm(entry.getKey()));

    // add term
    instance.setTermFrequency(entry.getKey(), entry.getValue());
    // must be found
    assertEquals(true, instance.containsTerm(entry.getKey()));
  }

  /**
   * Test of setTermFrequency method, of class DefaultDocumentModel.
   */
  @Test
  public void testSetTermFrequency() {
    TestUtility.logHeader(LOG, "setTermFrequency");

    final Entry<String, Long> entry = getRandomTermEntry();
    final DefaultDocumentModel instance = createModelInstance();

    instance.setTermFrequency(entry.getKey(), entry.getValue());
  }

  /**
   * Test of getDocId method, of class DefaultDocumentModel.
   */
  @Test
  public void testGetDocId() {
    TestUtility.logHeader(LOG, "getDocId");

    final DefaultDocumentModel instance = createModelInstance();
    final int result = instance.getDocId();

    assertEquals(defaultDocId, result);
  }

  /**
   * Test of setDocId method, of class DefaultDocumentModel.
   */
  @Test
  public void testSetDocId() {
    TestUtility.logHeader(LOG, "setDocId");

    final DefaultDocumentModel instance = createModelInstance();
    final int expResult = 328;

    instance.setDocId(expResult);

    final int result = instance.getDocId();

    assertEquals(expResult, result);
  }

  /**
   * Test of getTermFrequency (whole index) method, of class
   * DefaultDocumentModel.
   */
  @Test
  public void testGetTermFrequencyAll() {
    TestUtility.logHeader(LOG, "getTermFrequency - overall");

    DefaultDocumentModel instance = createModelInstance();

    LOG.info("Try empty model.");
    // no terms - no frequency
    assertEquals(0L, instance.getTermFrequency());

    LOG.info("Try model with data.");
    addTermsToModel(instance);
    assertEquals(termFreq, instance.getTermFrequency());
  }

  /**
   * Test of getTermFrequency method, of class DefaultDocumentModel.
   */
  @Test
  public void testGetTermFrequencyForTerm() {
    TestUtility.logHeader(LOG, "getTermFrequency - term");

    final DefaultDocumentModel instance = createModelInstance();
    final Entry<String, Long> entry = getRandomTermEntry();

    LOG.info("Try empty model.");
    // no terms - no frequency
    assertEquals(0L, instance.getTermFrequency("foo"));

    LOG.info("Try model with data.");
    addTermsToModel(instance);
    assertEquals((Long) entry.getValue(), (Long) instance.getTermFrequency(
            entry.getKey()));
  }

}
