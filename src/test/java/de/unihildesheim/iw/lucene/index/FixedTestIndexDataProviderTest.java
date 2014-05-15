/*
 * Copyright (C) 2014 bhoerdzn
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
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.RandomValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class FixedTestIndexDataProviderTest
    extends TestCase {

  /**
   * Global singleton instance.
   */
  private static final FixedTestIndexDataProvider INSTANCE =
      FixedTestIndexDataProvider.getInstance();

  @Test
  public void testDocumentContains()
      throws Exception {
    final Map<Integer, String> words = new HashMap<>(10);
    // remember: all terms are lower-cased!
    words.put(0, "lorem");
    words.put(1, "neque");
    words.put(2, "mollis");
    words.put(3, "phasellus");
    words.put(4, "turpis");
    words.put(5, "sagittis");
    words.put(6, "rhoncus");
    words.put(7, "interdum");
    words.put(8, "quisque");
    words.put(9, "molestie");

    // check words that must be there
    for (Map.Entry<Integer, String> check : words.entrySet()) {
      Assert.assertTrue(
          "Term not found. doc=" + check.getKey() + " term=" + check.getValue(),
          INSTANCE.documentContains(check.getKey(),
              new ByteArray(check.getValue().getBytes("UTF-8")))
      );
    }

    // term not in documents
    final ByteArray someTerm = new ByteArray("foo".getBytes("UTF-8"));

    // check terms that must not be found
    for (int i = 0; i < 10; i++) {
      Assert.assertFalse("Unexpected term found.", INSTANCE.documentContains
          (i, someTerm));
    }

    // run out of documents
    for (int i = 0; i < 10; i++) {
      try {
        INSTANCE.documentContains(i, someTerm);
      } catch (IllegalArgumentException e) {
        if (i < INSTANCE.getDocumentCount()) {
          Assert.fail("Unexpected exception caught.");
        } else {
          // ok
          break;
        }
      }
    }
  }

  @Test
  public void testGetStopwords() {
    Assert.assertTrue("Stopwords should always be empty.",
        INSTANCE.getStopwords().isEmpty());
  }

  @Test
  public void testDispose() {
    INSTANCE.dispose();
  }

  @Test
  public void testIsDisposed() {
    Assert.assertFalse("Instance is able to be disposed.",
        INSTANCE.isDisposed());
    INSTANCE.dispose();
    Assert.assertFalse("Instance is able to be disposed.",
        INSTANCE.isDisposed());
  }

  @Test
  public void testGetDocumentFields() {
    final Set<String> fields = INSTANCE.getDocumentFields();
    Assert.assertFalse("Empty fields list.", fields.isEmpty());
    Assert.assertEquals("Field count mismatch.",
        FixedTestIndexDataProvider.FIELD_COUNT,
        fields.size());
  }

  @Test
  public void testGetLastIndexCommitGeneration() {
    Assert.assertEquals("Commit generation should be zero.",
        0L, INSTANCE.getLastIndexCommitGeneration().longValue());
  }

  @Test
  public void testGetDocumentCount() {
    Assert.assertEquals("Document count mismatch.",
        FixedTestIndexDataProvider.DOC_COUNT, INSTANCE.getDocumentCount());
  }

  @Test
  public void testHasDocument() {
    for (int i = -10; ; i++) {
      if (INSTANCE.hasDocument(i) &&
          (i < 0 || i >= FixedTestIndexDataProvider.DOC_COUNT)) {
        Assert.fail("Unexpected document id found. id=" + i);
      } else if (!INSTANCE.hasDocument(i) && i >=
          FixedTestIndexDataProvider.DOC_COUNT) {
        break;
      } else if (!INSTANCE.hasDocument(i) && i < 0) {
        continue;
      } else {
        Assert.assertTrue("Document id not found. id=" + i,
            INSTANCE.hasDocument(i));
      }
    }
  }

  @Test
  public void testGetDocumentsTermSet() {
    final Set<Integer> docIds = new HashSet<>(
        FixedTestIndexDataProvider.DOC_COUNT);
    for (int i = 0; i < FixedTestIndexDataProvider.DOC_COUNT; i++) {
      docIds.add(RandomValue.getInteger(0, FixedTestIndexDataProvider
          .DOC_COUNT - 1));
    }

    final Set<ByteArray> docTerms;
    docTerms = INSTANCE.getDocumentsTermSet(docIds);
    boolean found = false;
    for (ByteArray term : docTerms) {
      for (Integer docId : docIds) {
        if (INSTANCE.documentContains(docId, term)) {
          found = true;
          break;
        }
      }
      if (!found) {
        Assert.fail("Term not found. t=" + ByteArrayUtils.utf8ToString(term) +
                " docIds=" + docIds
        );
      }
      found = false;
    }
  }

  @Test
  public void testGetDocumentModel() {
    for (int i = 0; i < FixedTestIndexDataProvider.DOC_COUNT; i++) {
      final DocumentModel docModel = INSTANCE.getDocumentModel(i);
      for (ByteArray term : docModel.termFreqMap.keySet()) {
        Assert.assertTrue("Term in model, but not in index.", INSTANCE
            .documentContains(i, term));
      }
    }
  }

  @Test
  public void testGetTermFrequency_term() {
    final Iterator<ByteArray> termsIt = INSTANCE.getTermsIterator();
    while (termsIt.hasNext()) {
      final ByteArray term = termsIt.next();
      int matchedDocs = 0;
      for (int docId = 0; docId < FixedTestIndexDataProvider.DOC_COUNT;
           docId++) {
        if (INSTANCE.documentContains(docId, term)) {
          matchedDocs++;
        }
      }
      final long tf = INSTANCE.getTermFrequency(term);
      if (matchedDocs > tf) {
        Assert
            .fail("Matched docs > term-frequency. md=" + matchedDocs + " tf=" +
                tf);
      }
    }
  }
}