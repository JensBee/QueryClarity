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
import de.unihildesheim.lucene.util.BytesWrap;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.lucene.util.BytesRef;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DefaultDocumentModelTest {

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

  /**
   * Create a new {@link DefaultDocumentModel} set-up for populating with the
   * default terms.
   *
   * @return Test {@link DefaultDocumentModel} instance
   */
  private DocumentModel createModelInstance() {
    return new DefaultDocumentModel(defaultDocId,
            DefaultDocumentModelTest.terms.size());
  }

  private BytesWrap bytesWrapString(final String str) {
    return BytesWrap.wrap(new BytesRef(str));
  }

  /**
   * Add all default terms to the given document model.
   *
   * @param docModel Model to add the terms to
   */
  private void addTermsToModel(DocumentModel docModel) {
    for (Entry<String, Long> entry : DefaultDocumentModelTest.terms.entrySet()) {
      docModel.setTermFrequency(bytesWrapString(entry.getKey()), entry.getValue());
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
    final String term = docTerms[TestUtility.getRandInt(0,
            docTerms.length - 1)];

    return new AbstractMap.SimpleEntry<>(term, DefaultDocumentModelTest.terms.
            get(term));
  }

  /**
   * Test of setTermData method, of class DefaultDocumentModel.
   */
  @Test
  public void testAddTermData() {
    TestUtility.logHeader(LOG, "addTermData");

    final Entry<String, Long> entry = getRandomTermEntry();
    final String key = "test";
    final Number value = 123;

    LOG.info("Adding v={} k={} to t={}", value, key, entry);

    final DocumentModel instance = createModelInstance();
    instance.setTermData(bytesWrapString(entry.getKey()), key, value);
  }

  /**
   * Test of getTermData method, of class DefaultDocumentModel.
   */
  @Test
  public void testGetTermData() {
    TestUtility.logHeader(LOG, "getTermData");

    final Entry<String, Long> entry = getRandomTermEntry();
    final String key = "test";
    final Number value = 123;

    LOG.info("Adding v={} k={} to t={}", value, key, entry.getKey());

    final BytesWrap term = bytesWrapString(entry.getKey());
    DocumentModel instance = createModelInstance();
    instance.setTermData(term, key, value);

    final Number result = instance.getTermData(term, key);
    final Object expResult = value;

    LOG.info("Result v={} for k={} at t={}", result, key, entry.getKey());

    assertEquals(expResult, result);
  }

  /**
   * Test of containsTerm method, of class DefaultDocumentModel.
   */
  @Test
  public void testContainsTerm() {
    TestUtility.logHeader(LOG, "containsTerm");

    final Entry<String, Long> entry = getRandomTermEntry();
    DocumentModel instance = createModelInstance();

    // no terms stored
    BytesWrap term = bytesWrapString(entry.getKey());
    LOG.info("Contains t={} on empty model b={}", entry.getKey(), term.getBytes());
    assertEquals(false, instance.containsTerm(term));
    // add term
    LOG.info("Add t={} with f={}", entry.getKey(), entry.getValue());
    instance.setTermFrequency(term, entry.getValue());
    // must be found
    LOG.info("Contains t={}? r={}", entry.getKey(), instance.containsTerm(term));
    assertEquals(true, instance.containsTerm(bytesWrapString(entry.getKey())));
  }

  /**
   * Test of setTermFrequency method, of class DefaultDocumentModel.
   */
  @Test
  public void testAddTermFrequency() {
    TestUtility.logHeader(LOG, "addTermFrequency");

    final Entry<String, Long> entry = getRandomTermEntry();
    final BytesWrap term = bytesWrapString(entry.getKey());
    LOG.info("Add t={} with f={}", entry.getKey(), entry.getValue());
    @SuppressWarnings("DLS_DEAD_LOCAL_STORE")
    final DocumentModel instance = createModelInstance();
    instance.setTermFrequency(term, entry.getValue());
  }

  /**
   * Test of getDocId method, of class DefaultDocumentModel.
   */
  @Test
  public void testGetDocId() {
    TestUtility.logHeader(LOG, "getDocId");

    final DocumentModel instance = createModelInstance();
    LOG.info("Lookig for default id={}", defaultDocId);
    final int result = instance.getDocId();
    assertEquals(defaultDocId, result);
  }

  /**
   * Test of setDocId method, of class DefaultDocumentModel.
   */
  @Test
  public void testSetDocId() {
    TestUtility.logHeader(LOG, "setDocId");

    final int expResult = 328;
    LOG.info("Set id={}", expResult);
    DocumentModel instance = createModelInstance();
    instance.setDocId(expResult);
    final int result = instance.getDocId();
    LOG.info("Got id={}", result);
    assertEquals(expResult, result);
  }

  /**
   * Test of getTermFrequency (whole index) method, of class
   * DefaultDocumentModel.
   */
  @Test
  public void testGetTermFrequency_0args() {
    TestUtility.logHeader(LOG, "getTermFrequency - overall");

    DocumentModel instance = createModelInstance();

    LOG.info("Try empty model.");
    // no terms - no frequency
    assertEquals(0L, instance.getTermFrequency());

    LOG.info("Try model with data.");
    addTermsToModel(instance);
    assertEquals(termFreq, instance.getTermFrequency());
  }

  /**
   * Test of create method, of class DefaultDocumentModel.
   */
  @Test
  public void testCreate() {
    TestUtility.logHeader(LOG, "create");
    DocumentModel instance = new DefaultDocumentModel();
    instance.create(defaultDocId, 0);
    assertEquals(defaultDocId, instance.getDocId());
  }

}
