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

import de.unihildesheim.util.StringUtils;
import de.unihildesheim.lucene.TestUtility;
import de.unihildesheim.lucene.index.MemoryIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class DocFieldsTermsEnumTest {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DocFieldsTermsEnumTest.class);

  /**
   * Reader to access a test index.
   */
  private static IndexReader reader;

  /**
   * Index used by this test.
   */
  private static MemoryIndex idx;

  @BeforeClass
  public static void setUpClass() throws IOException {
    // test index data
    final String[] fields = new String[]{"title", "text", "id"};
    final List<String[]> documents = new ArrayList<String[]>(4);
    documents.add(new String[]{"A Book", "Lucene in Action", "title1"});
    documents.add(new String[]{"Another book", "Lucene for Dummies", "title2"});
    documents.add(new String[]{"Just a bunch of papers", "Managing Gigabytes",
      "title3"});
    documents.add(new String[]{"Literature", "The Art of Computer Science",
      "title4"});
    idx = new MemoryIndex(fields, documents);
    reader = idx.getReader();
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
   * Get a new {@link DocFieldsTermsEnum} instance with all default fields
   * enabled.
   *
   * @return Instance
   */
  private DocFieldsTermsEnum getFullInstance() {
    return new DocFieldsTermsEnum(reader, idx.getIdxFields());
  }

  /**
   * Get a new {@link DocFieldsTermsEnum} instance with all default fields
   * enabled and a random document set as current.
   *
   * @return Instance
   */
  private DocFieldsTermsEnum getInstance() {
    final List<Integer> docIds = new ArrayList(idx.getDocumentIds());
    final int documentId = docIds.get(TestUtility.getRandInt(0, docIds.size()));

    return new DocFieldsTermsEnum(reader, idx.getIdxFields(), documentId);
  }

  /**
   * Test of setDocument method, of class DocFieldsTermsEnum.
   */
  @Test
  public final void testSetDocument() {
    TestUtility.logHeader(LOG, "setDocument");

    final List<Integer> docIds = new ArrayList(idx.getDocumentIds());
    final int documentId = docIds.get(TestUtility.getRandInt(0, docIds.size()));

    DocFieldsTermsEnum instance = getInstance();

    instance.setDocument(documentId);
  }

  /**
   * Test of reset method, of class DocFieldsTermsEnum.
   *
   * @throws java.io.IOException Thrown on low-level I/O errors
   */
  @Test
  public final void testReset() throws IOException {
    TestUtility.logHeader(LOG, "reset");

    final DocFieldsTermsEnum instance = getInstance();

    final BytesRef expResult = instance.next(); // get first term
    LOG.info("First term={}", expResult.utf8ToString());

    // forward to next term, may be null if there's only one term
    final BytesRef nextTerm = instance.next();
    LOG.info("Next term={}", nextTerm.utf8ToString());
    // reset to start
    LOG.info("Reset");
    instance.reset();

    // re-get first term
    final BytesRef firstTerm = instance.next();
    LOG.info("First term again term={}", firstTerm.utf8ToString());
    assertEquals("Should be same value.", expResult, firstTerm);
  }

  /**
   * Test of next method, of class DocFieldsTermsEnum.
   */
  @Test
  public void testNext() throws Exception {
    TestUtility.logHeader(LOG, "next");

    final List<Integer> docIds = new ArrayList(idx.getDocumentIds());
    final int documentId = docIds.get(TestUtility.getRandInt(0, docIds.size()));

    final String field = idx.getIdxFields()[0];
    LOG.info("Testing values of field={} for docId={}", field, documentId);
    final String[] values = reader.document(documentId).get(field).split("\\s+");

    final List<String> valSet = new ArrayList();
    for (String term : values) {
      valSet.add(StringUtils.lowerCase(term));
    }
    LOG.info("Expected values {}", valSet);

    final DocFieldsTermsEnum instance = new DocFieldsTermsEnum(reader,
            new String[]{field}, documentId);

    final List<String> foundValSet = new ArrayList(valSet.size());
    BytesRef bRef = instance.next();
    while (bRef != null) {
      final String term = bRef.utf8ToString();
      foundValSet.add(StringUtils.lowerCase(term));
      bRef = instance.next();
    }

    LOG.info("Found values {}", foundValSet);

    assertEquals(true, valSet.containsAll(foundValSet));
  }

  /**
   * Test of getTotalTermFreq method, of class DocFieldsTermsEnum.
   */
  @Test
  public void testGetTotalTermFreq() throws Exception {
    TestUtility.logHeader(LOG, "getTotalTermFreq");

    final List<Integer> docIds = new ArrayList(idx.getDocumentIds());
    final int documentId = docIds.get(TestUtility.getRandInt(0, docIds.size()));

    LOG.info("Conting terms for docId={}", documentId);

    int termCount = 0;
    for (String field : idx.getIdxFields()) {
      final String content = reader.document(documentId).get(field);
      final int count = content.split("\\s+").length;
      LOG.info("field={} terms=[{}] count={}", field, content, count);
      termCount += count;
    }
    LOG.info("Terms for docId={}", termCount);

    final DocFieldsTermsEnum instance = new DocFieldsTermsEnum(reader,
            idx.getIdxFields(), documentId);

    // iterate all terms
    int enumCount = 0;
    BytesRef bRef = instance.next();
    while (bRef != null) {
      LOG.info("term={} count={}", bRef.utf8ToString(), instance.
              getTotalTermFreq());
      enumCount += instance.getTotalTermFreq();
      bRef = instance.next();
    }

    LOG.info("Terms reported for docId={}", enumCount);

    // now allover value is known
    assertEquals(termCount, enumCount);
  }

}
