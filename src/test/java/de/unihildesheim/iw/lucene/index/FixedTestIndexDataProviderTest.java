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

import java.util.HashSet;
import java.util.Set;

public class FixedTestIndexDataProviderTest
    extends TestCase {

  /**
   * Global singleton instance.
   */
  private static final FixedTestIndexDataProvider INSTANCE =
      FixedTestIndexDataProvider.getInstance();

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
        FixedTestIndexDataProvider.KnownData.FIELD_COUNT,
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
        FixedTestIndexDataProvider.KnownData.DOC_COUNT,
        INSTANCE.getDocumentCount());
  }

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
        FixedTestIndexDataProvider.KnownData.DOC_COUNT);
    for (int i = 0; i < FixedTestIndexDataProvider.KnownData.DOC_COUNT; i++) {
      docIds.add(RandomValue.getInteger(0, FixedTestIndexDataProvider.KnownData.
          DOC_COUNT - 1));
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
    for (int i = 0; i < FixedTestIndexDataProvider.KnownData.DOC_COUNT; i++) {
      final DocumentModel docModel = INSTANCE.getDocumentModel(i);
      for (ByteArray term : docModel.termFreqMap.keySet()) {
        Assert.assertTrue("Term in model, but not in index.", INSTANCE
            .documentContains(i, term));
      }
    }
  }
}