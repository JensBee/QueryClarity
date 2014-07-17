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
import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.RandomValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Test for {@link FixedTestIndexDataProvider}.
 *
 * @author Jens Bertram
 */
public final class FixedTestIndexDataProviderTest
    extends TestCase {

  /**
   * Global singleton instance.
   */
  private static final FixedTestIndexDataProvider INSTANCE =
      FixedTestIndexDataProvider.getInstance();

  /**
   * Test getting stopwords. They should never be set.
   */
  @Test
  public void testGetStopwords() {
    Assert.assertTrue("Stopwords should always be empty.",
        INSTANCE.getStopwords().isEmpty());
  }

  /**
   * Test getting fields.
   */
  @Test
  public void testGetDocumentFields() {
    final Set<String> fields = INSTANCE.getDocumentFields();
    Assert.assertFalse("Empty fields list.", fields.isEmpty());
    Assert.assertEquals("Field count mismatch.",
        (long) FixedTestIndexDataProvider.KnownData.FIELD_COUNT,
        (long) fields.size());
  }

  /**
   * Test document frequency values.
   *
   * @throws Exception Any exception thrown indicates an error.
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Test
  public void testGetDocumentFrequency()
      throws Exception {
    for (final String term : FixedTestIndexDataProvider.KnownData.IDX_TERMFREQ
        .keySet()) {
      Assert.assertEquals("Document frequency mismatch. t=" + term,
          (long) FixedTestIndexDataProvider.KnownData
              .IDX_DOCFREQ.get(term),
          (long) INSTANCE.getDocumentFrequency(
              new ByteArray(term.getBytes("UTF-8")))
      );
    }
  }

  /**
   * Test commit generation. Should always be zero.
   */
  @Test
  public void testGetLastIndexCommitGeneration() {
    Assert.assertEquals("Commit generation should be zero.",
        0L, INSTANCE.getLastIndexCommitGeneration().longValue());
  }

  /**
   * Test getting the number of documents in index.
   */
  @Test
  public void testGetDocumentCount() {
    Assert.assertEquals("Document count mismatch.",
        (long) FixedTestIndexDataProvider.KnownData.DOC_COUNT,
        INSTANCE.getDocumentCount());
  }

  /**
   * Test if all documents are found.
   */
  @SuppressWarnings("StatementWithEmptyBody")
  @Test
  public void testHasDocument() {
    for (int i = -10; ; i++) {
      if (INSTANCE.hasDocument(i) &&
          (i < 0 || i >= FixedTestIndexDataProvider.KnownData.DOC_COUNT)) {
        Assert.fail("Unexpected document id found. id=" + i);
      } else if (!INSTANCE.hasDocument(i) && i >=
          FixedTestIndexDataProvider.KnownData.DOC_COUNT) {
        break;
      } else if (!INSTANCE.hasDocument(i) && i < 0) {
        // pass
      } else {
        Assert.assertTrue("Document id not found. id=" + i,
            INSTANCE.hasDocument(i));
      }
    }
  }

  /**
   * Test getting unique document terms.
   */
  @Test
  public void testGetDocumentsTermSet() {
    final Collection<Integer> docIds = new HashSet<>(
        FixedTestIndexDataProvider.KnownData.DOC_COUNT);
    for (int i = 0; i < FixedTestIndexDataProvider.KnownData.DOC_COUNT; i++) {
      docIds.add(RandomValue.getInteger(0, FixedTestIndexDataProvider.KnownData.
          DOC_COUNT - 1));
    }

    final Iterator<ByteArray> docTerms = INSTANCE.getDocumentsTermsSet(docIds);
    boolean found = false;
    while (docTerms.hasNext()) {
      final ByteArray term = docTerms.next();
      for (final Integer docId : docIds) {
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

  /**
   * Test getting document models.
   */
  @Test
  public void testGetDocumentModel() {
    for (int i = 0; i < FixedTestIndexDataProvider.KnownData.DOC_COUNT; i++) {
      final DocumentModel docModel = INSTANCE.getDocumentModel(i);
      for (final ByteArray term : docModel.getTermFreqMap().keySet()) {
        Assert.assertTrue("Term in model, but not in index.", INSTANCE
            .documentContains(i, term));
      }
    }
  }
}