/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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

import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.VecTextField;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader.Builder;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReaderTest
    .TestMemIndex.Index;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Test for {@link FilteredDirectoryReader}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class FilteredDirectoryReaderTest
    extends TestCase {
  public FilteredDirectoryReaderTest() {
    super(LoggerFactory.getLogger(FilteredDirectoryReaderTest.class));
  }

  /**
   * Test the plain builder.
   *
   * @throws Exception
   */
  @Test
  public void testBuilder_plain()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.PLAIN)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      new Builder(reader).build();
    }
  }

  /**
   * Test with a single field visible.
   *
   * @throws Exception
   */
  @SuppressWarnings({"ImplicitNumericConversion",
      "AnonymousInnerClassMayBeStatic"})
  @Test
  public void testBuilder_singleField()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.ALL_FIELDS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader)
          .fields(Collections.singleton("f2"))
          .build();

      new LeafReaderInstanceTest() {

        @Override
        void testHasDeletions()
            throws Exception {
          Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
        }

        @Override
        void testFieldCount() {
          Assert.assertEquals("Field count mismatch.",
              1L, fReader.getFields().size());
        }

        @Override
        void testFieldNames()
            throws Exception {
          Assert.assertTrue("Single visible field not found.",
              fReader.getFields().contains("f2"));
        }

        @Override
        void testTotalTermFreq()
            throws IOException {
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & term.",
              idx.docs, fReader.totalTermFreq(new Term("f2", "field2")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & missing term.",
              0L, fReader.totalTermFreq(new Term("f1", "foo")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden field & term.",
              0L, fReader.totalTermFreq(new Term("f1", "field1")));
        }

        @Override
        void testSumTotalTermFreq()
            throws Exception {
          Assert.assertEquals("SumTotalTermFreq mismatch for visible field.",
              18L, fReader.getSumTotalTermFreq("f2"));
          for (final String f : idx.flds) {
            if (!"f2".equals(f)) {
              Assert.assertEquals("SumTotalTermFreq mismatch for hidden field.",
                  0L, fReader.getSumTotalTermFreq(f));
            }
          }
        }

        @Override
        void testDocCount()
            throws Exception {
          Assert.assertEquals("Doc count mismatch for visible field.",
              idx.docs, fReader.getDocCount("f2"));
          // check visibility of all docs without visible fields
          for (final String f : idx.flds) {
            if (!"f2".equals(f)) {
              Assert.assertEquals("Document visible with hidden field.",
                  0L, fReader.getDocCount(f));
            }
          }
        }

        @Override
        void testDocFreq()
            throws Exception {
          Assert.assertEquals("Missing term from all documents.",
              idx.docs, fReader.docFreq(new Term("f2", "field2")));
        }

        @Override
        void testSumDocFreq()
            throws Exception {
          Assert.assertEquals("SumDocFreq mismatch for visible field.",
              18L, fReader.getSumDocFreq("f2"));
          for (final String f : idx.flds) {
            if (!"f2".equals(f)) {
              Assert.assertEquals("SumDocFreq has result for hidden field.",
                  0L, fReader.getSumDocFreq(f));
            }
          }
        }

        @Override
        void testTermVectors()
            throws Exception {
          for (int i = 0; i < idx.docs - 1; i++) {
            final Fields f = fReader.getTermVectors(i);
            Assert.assertEquals("Too much fields retrieved from TermVector.",
                1L, f.size());
            f.forEach(fld ->
                    Assert.assertTrue("Hidden field found.", "f2".equals(fld))
            );
          }
        }

        @Override
        void testNumDocs()
            throws Exception {
          Assert.assertEquals("NumDocs mismatch.", idx.docs, fReader.numDocs());
        }

        @Override
        void testMaxDoc()
            throws Exception {
          Assert.assertEquals("MaxDoc mismatch.", idx.docs, fReader.maxDoc());
        }
      };
    }
  }

  /**
   * Test with a single field negated.
   *
   * @throws Exception
   */
  @SuppressWarnings({"ImplicitNumericConversion",
      "AnonymousInnerClassMayBeStatic"})
  @Test
  public void testBuilder_singleField_negate()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.ALL_FIELDS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader)
          .fields(Collections.singleton("f2"), true) // f2 is hidden
          .build();

      new LeafReaderInstanceTest() {

        @Override
        void testHasDeletions()
            throws Exception {
          Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
        }

        @Override
        void testFieldCount()
            throws Exception {
          Assert.assertEquals("Field count mismatch.",
              idx.flds.size() - 1, fReader.getFields().size());
        }

        @Override
        void testFieldNames()
            throws Exception {
          Assert.assertFalse("Excluded field found.",
              fReader.getFields().contains("f2"));
        }

        @Override
        void testTotalTermFreq()
            throws Exception {
          // visible fields
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & term.",
              idx.docs, fReader.totalTermFreq(new Term("f1", "field1")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & term.",
              idx.docs, fReader.totalTermFreq(new Term("f3", "field3")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & missing term.",
              0L, fReader.totalTermFreq(new Term("f3", "foo")));
          // hidden fields
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden field & term.",
              0L, fReader.totalTermFreq(new Term("f2", "field2")));
        }

        @Override
        void testSumTotalTermFreq()
            throws Exception {
          Assert.assertEquals("SumTotalTermFreq mismatch for visible field.",
              18L, fReader.getSumTotalTermFreq("f1"));
          Assert.assertEquals("SumTotalTermFreq mismatch for visible field.",
              18L, fReader.getSumTotalTermFreq("f3"));
          Assert.assertEquals("SumTotalTermFreq mismatch for hidden field.",
              0L, fReader.getSumTotalTermFreq("f2"));
        }

        @Override
        void testDocCount()
            throws Exception {
          Assert.assertEquals("Doc count mismatch for visible field.",
              idx.docs, fReader.getDocCount("f1"));
          Assert.assertEquals("Doc count mismatch for visible field.",
              idx.docs, fReader.getDocCount("f3"));
          // check visibility of all docs without visible fields
          Assert.assertEquals("Document visible with hidden field.",
              0L, fReader.getDocCount("f2"));
        }

        @Override
        void testDocFreq()
            throws Exception {
          Assert.assertEquals("Missing term from all documents.",
              idx.docs, fReader.docFreq(new Term("f1", "field1")));
          Assert.assertEquals("Found term from hidden documents.",
              0L, fReader.docFreq(new Term("f2", "field2")));
          Assert.assertEquals("Missing term from all documents.",
              idx.docs, fReader.docFreq(new Term("f3", "field3")));
        }

        @Override
        void testSumDocFreq()
            throws Exception {
          Assert.assertEquals("SumDocFreq mismatch for visible field.",
              18L, fReader.getSumDocFreq("f1"));
          Assert.assertEquals("SumDocFreq mismatch for visible field.",
              18L, fReader.getSumDocFreq("f3"));
          Assert.assertEquals("SumDocFreq has result for hidden field.",
              0L, fReader.getSumDocFreq("f2"));
        }

        @Override
        void testTermVectors()
            throws Exception {
          for (int i = 0; i < idx.docs - 1; i++) {
            final Fields f = fReader.getTermVectors(i);
            Assert.assertEquals("Too much fields retrieved from TermVector.",
                2L, f.size());
            f.forEach(fld ->
                    Assert.assertFalse("Hidden field found.", "f2".equals(fld))
            );
          }
        }

        @Override
        void testNumDocs()
            throws Exception {
          Assert.assertEquals("NumDocs mismatch.",
              idx.docs, fReader.numDocs());
        }

        @Override
        void testMaxDoc()
            throws Exception {
          Assert.assertEquals("MaxDoc mismatch.",
              idx.docs, fReader.maxDoc());
        }
      };
    }
  }

  /**
   * Test with all fields negated (-> no field visible).
   *
   * @throws Exception
   */
  @SuppressWarnings({"ImplicitNumericConversion",
      "AnonymousInnerClassMayBeStatic"})
  @Test
  public void testBuilder_allFields_negate()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.ALL_FIELDS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader)
          .fields(idx.flds, true)
          .build();

      new LeafReaderInstanceTest() {

        @Override
        void testHasDeletions()
            throws Exception {
          Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
        }

        @Override
        void testFieldCount()
            throws Exception {
          Assert.assertEquals("Field count mismatch.",
              0, fReader.getFields().size());
        }

        @Override
        void testFieldNames()
            throws Exception {
          // NOP, no fields
        }

        @Override
        void testTotalTermFreq()
            throws Exception {
          // hidden fields
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden field & term.",
              0L, fReader.totalTermFreq(new Term("f1", "field1")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden field & term.",
              0L, fReader.totalTermFreq(new Term("f2", "field2")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden field & term.",
              0L, fReader.totalTermFreq(new Term("f3", "field3")));
        }

        @Override
        void testSumTotalTermFreq()
            throws Exception {
          Assert.assertEquals("SumTotalTermFreq mismatch for hidden field.",
              0L, fReader.getSumTotalTermFreq("f1"));
          Assert.assertEquals("SumTotalTermFreq mismatch for hidden field.",
              0L, fReader.getSumTotalTermFreq("f2"));
          Assert.assertEquals("SumTotalTermFreq mismatch for hidden field.",
              0L, fReader.getSumTotalTermFreq("f3"));
        }

        @Override
        void testDocCount()
            throws Exception {
          Assert.assertEquals("Doc count mismatch for hidden field.",
              0L, fReader.getDocCount("f1"));
          Assert.assertEquals("Doc count mismatch for hidden field.",
              0L, fReader.getDocCount("f2"));
          Assert.assertEquals("Doc count mismatch for hidden field.",
              0L, fReader.getDocCount("f3"));
        }

        @Override
        void testDocFreq()
            throws Exception {
          Assert.assertEquals("Found term from hidden documents.",
              0L, fReader.docFreq(new Term("f1", "field1")));
          Assert.assertEquals("Found term from hidden documents.",
              0L, fReader.docFreq(new Term("f2", "field2")));
          Assert.assertEquals("Found term from hidden documents.",
              0L, fReader.docFreq(new Term("f3", "field3")));
        }

        @Override
        void testSumDocFreq()
            throws Exception {
          Assert.assertEquals("SumDocFreq mismatch for hidden field.",
              0L, fReader.getSumDocFreq("f1"));
          Assert.assertEquals("SumDocFreq mismatch for hidden field.",
              0L, fReader.getSumDocFreq("f2"));
          Assert.assertEquals("SumDocFreq mismatch for hidden field.",
              0L, fReader.getSumDocFreq("f3"));
        }

        @Override
        void testTermVectors()
            throws Exception {
          // NOP, no documents
        }

        @Override
        void testNumDocs()
            throws Exception {
          Assert.assertEquals("NumDocs mismatch.", 0L, fReader.numDocs());
        }

        @Override
        void testMaxDoc()
            throws Exception {
          Assert.assertEquals("MaxDoc mismatch.", 1L, fReader.maxDoc());
        }
      };
    }
  }

  /**
   * Test with all fields enabled.
   *
   * @throws Exception
   */
  @SuppressWarnings({"ImplicitNumericConversion",
      "AnonymousInnerClassMayBeStatic"})
  @Test
  public void testBuilder_allFields()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.ALL_FIELDS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader)
          .fields(idx.flds)
          .build();

      new LeafReaderInstanceTest() {

        @Override
        void testHasDeletions()
            throws Exception {
          Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
        }

        @Override
        void testFieldCount()
            throws Exception {
          Assert.assertEquals("Field count mismatch.",
              idx.flds.size(), fReader.getFields().size());
        }

        @Override
        void testFieldNames()
            throws Exception {
          Assert.assertTrue("Visible field not found.",
              fReader.getFields().containsAll(idx.flds));
        }

        @SuppressWarnings("ObjectAllocationInLoop")
        @Override
        void testTotalTermFreq()
            throws Exception {
          for (final String f : idx.flds) {
            Assert.assertEquals(
                "TotalTermFreq mismatch for visible field & term.",
                idx.docs, fReader.totalTermFreq(new Term(f, "value")));
          }
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & missing term.",
              0L, fReader.totalTermFreq(new Term("f1", "foo")));
        }

        @Override
        void testSumTotalTermFreq()
            throws Exception {
          for (final String f : idx.flds) {
            Assert.assertEquals("SumTotalTermFreq mismatch for visible field.",
                18L, fReader.getSumTotalTermFreq(f));
          }
        }

        @Override
        void testDocCount()
            throws Exception {
          Assert.assertEquals("Doc count mismatch for visible field.",
              idx.docs, fReader.getDocCount("f2"));
        }

        @SuppressWarnings("ObjectAllocationInLoop")
        @Override
        void testDocFreq()
            throws Exception {
          for (final String f : idx.flds) {
            Assert.assertEquals("Missing term from all documents.",
                idx.docs, fReader.docFreq(new Term(f, "value")));
          }
        }

        @Override
        void testSumDocFreq()
            throws Exception {
          for (final String f : idx.flds) {
            Assert.assertEquals("Missing term from all documents.",
                18L, fReader.getSumDocFreq(f));
          }
        }

        @Override
        void testTermVectors()
            throws Exception {
          for (int i = 0; i < idx.docs - 1; i++) {
            final Fields f = fReader.getTermVectors(i);
            Assert.assertEquals("Too much fields retrieved from TermVector.",
                idx.flds.size(), f.size());
          }
        }

        @Override
        void testNumDocs()
            throws Exception {
          Assert.assertEquals("NumDocs mismatch.", idx.docs, fReader.numDocs());
        }

        @Override
        void testMaxDoc()
            throws Exception {
          Assert.assertEquals("MaxDoc mismatch.", idx.docs, fReader.maxDoc());
        }
      };
    }
  }

  /**
   * Test specifying multiple fields to include.
   *
   * @throws Exception
   */
  @SuppressWarnings({"ImplicitNumericConversion",
      "AnonymousInnerClassMayBeStatic"})
  @Test
  public void testBuilder_multiFields()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.ALL_FIELDS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);

      final FilteredDirectoryReader fReader = new Builder(reader)
          .fields(Arrays.asList("f1", "f3"))
          .build();

      Assert.assertEquals("Field count mismatch.",
          2, fReader.getFields().size());

      new LeafReaderInstanceTest() {

        @Override
        void testHasDeletions()
            throws Exception {
          Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
        }

        @Override
        void testFieldCount()
            throws Exception {
          Assert.assertEquals("Field count mismatch.",
              2L, fReader.getFields().size());
        }

        @Override
        void testFieldNames()
            throws Exception {
          Assert.assertFalse("Hidden field found.",
              fReader.getFields().contains("f2"));
        }

        @Override
        void testTotalTermFreq()
            throws Exception {
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & term.",
              idx.docs, fReader.totalTermFreq(new Term("f1", "field1")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & term.",
              idx.docs, fReader.totalTermFreq(new Term("f3", "field3")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & missing term.",
              0L, fReader.totalTermFreq(new Term("f1", "foo")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden field & term.",
              0L, fReader.totalTermFreq(new Term("f2", "field2")));
        }

        @Override
        void testSumTotalTermFreq()
            throws Exception {
          Assert.assertEquals("SumTotalTermFreq mismatch for visible field.",
              18L, fReader.getSumTotalTermFreq("f1"));
          Assert.assertEquals("SumTotalTermFreq mismatch for visible field.",
              18L, fReader.getSumTotalTermFreq("f3"));

          Assert.assertEquals("SumTotalTermFreq mismatch for hidden field.",
              0L, fReader.getSumTotalTermFreq("f2"));
        }

        @Override
        void testDocCount()
            throws Exception {
          Assert.assertEquals("Doc count mismatch for visible field.",
              idx.docs, fReader.getDocCount("f1"));
          Assert.assertEquals("Doc count mismatch for visible field.",
              idx.docs, fReader.getDocCount("f3"));

          Assert.assertEquals("Document visible with hidden field.",
              0L, fReader.getDocCount("f2"));
        }

        @Override
        void testDocFreq()
            throws Exception {
          Assert.assertEquals("Missing term from all documents.",
              idx.docs, fReader.docFreq(new Term("f1", "field1")));
          Assert.assertEquals("Missing term from all documents.",
              idx.docs, fReader.docFreq(new Term("f3", "field3")));

          Assert.assertEquals("Found term from hidden documents.",
              0L, fReader.docFreq(new Term("f2", "field2")));
        }

        @Override
        void testSumDocFreq()
            throws Exception {
          Assert.assertEquals("SumDocFreq mismatch for visible field.",
              18L, fReader.getSumDocFreq("f1"));
          Assert.assertEquals("SumDocFreq mismatch for visible field.",
              18L, fReader.getSumDocFreq("f3"));

          Assert.assertEquals("SumDocFreq has result for hidden field.",
              0L, fReader.getSumDocFreq("f2"));
        }

        @Override
        void testTermVectors()
            throws Exception {
          for (int i = 0; i < idx.docs - 1; i++) {
            final Fields f = fReader.getTermVectors(i);
            Assert.assertEquals("Too much fields retrieved from TermVector.",
                2L, f.size());
            f.forEach(fld ->
                    Assert.assertFalse("Hidden field found.", "f2".equals(fld))
            );
          }
        }

        @Override
        void testNumDocs()
            throws Exception {
          Assert.assertEquals("NumDocs mismatch.", idx.docs, fReader.numDocs());
        }

        @Override
        void testMaxDoc()
            throws Exception {
          Assert.assertEquals("MaxDoc mismatch.", idx.docs, fReader.maxDoc());
        }
      };
    }
  }

  /**
   * Test negated field definition with multiple fields.
   *
   * @throws Exception
   */
  @SuppressWarnings({"ImplicitNumericConversion",
      "AnonymousInnerClassMayBeStatic"})
  @Test
  public void testBuilder_multiFields_negate()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.ALL_FIELDS)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);

      final FilteredDirectoryReader fReader = new Builder(reader)
          .fields(Arrays.asList("f1", "f3"), true)
          .build();

      new LeafReaderInstanceTest() {

        @Override
        void testHasDeletions()
            throws Exception {
          Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
        }

        @Override
        void testFieldCount() {
          Assert.assertEquals("Field count mismatch.",
              1L, fReader.getFields().size());
        }

        @Override
        void testFieldNames()
            throws Exception {
          Assert.assertTrue("Single visible field not found.",
              fReader.getFields().contains("f2"));
          Assert.assertFalse("Excluded field found.",
              fReader.getFields().contains("f1"));
          Assert.assertFalse("Excluded field found.",
              fReader.getFields().contains("f3"));
        }

        @Override
        void testTotalTermFreq()
            throws IOException {
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & term.",
              idx.docs, fReader.totalTermFreq(new Term("f2", "field2")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & missing term.",
              0L, fReader.totalTermFreq(new Term("f1", "foo")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden field & term.",
              0L, fReader.totalTermFreq(new Term("f1", "field1")));
        }

        @Override
        void testSumTotalTermFreq()
            throws Exception {
          Assert.assertEquals("SumTotalTermFreq mismatch for visible field.",
              18L, fReader.getSumTotalTermFreq("f2"));
          for (final String f : idx.flds) {
            if (!"f2".equals(f)) {
              Assert.assertEquals("SumTotalTermFreq mismatch for hidden field.",
                  0L, fReader.getSumTotalTermFreq(f));
            }
          }
        }

        @Override
        void testDocCount()
            throws Exception {
          Assert.assertEquals("Doc count mismatch for visible field.",
              idx.docs, fReader.getDocCount("f2"));
          // check visibility of all docs without visible fields
          for (final String f : idx.flds) {
            if (!"f2".equals(f)) {
              Assert.assertEquals("Document visible with hidden field.",
                  0L, fReader.getDocCount(f));
            }
          }
        }

        @Override
        void testDocFreq()
            throws Exception {
          Assert.assertEquals("Missing term from all documents.",
              idx.docs, fReader.docFreq(new Term("f2", "field2")));
        }

        @Override
        void testSumDocFreq()
            throws Exception {
          Assert.assertEquals("SumDocFreq mismatch for visible field.",
              18L, fReader.getSumDocFreq("f2"));
          for (final String f : idx.flds) {
            if (!"f2".equals(f)) {
              Assert.assertEquals("SumDocFreq has result for hidden field.",
                  0L, fReader.getSumDocFreq(f));
            }
          }
        }

        @Override
        void testTermVectors()
            throws Exception {
          for (int i = 0; i < idx.docs - 1; i++) {
            final Fields f = fReader.getTermVectors(i);
            Assert.assertEquals("Too much fields retrieved from TermVector.",
                1L, f.size());
            f.forEach(fld ->
                    Assert.assertTrue("Hidden field found.", "f2".equals(fld))
            );
          }
        }

        @Override
        void testNumDocs()
            throws Exception {
          Assert.assertEquals("NumDocs mismatch.", idx.docs, fReader.numDocs());
        }

        @Override
        void testMaxDoc()
            throws Exception {
          Assert.assertEquals("MaxDoc mismatch.", idx.docs, fReader.maxDoc());
        }
      };
    }
  }

  /**
   * Test basic {@link TermFilter} usage.
   *
   * @throws Exception
   */
  @SuppressWarnings({"AnonymousInnerClassMayBeStatic",
      "ImplicitNumericConversion"})
  @Test
  public void testBuilder_termFilter()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.PLAIN)) {
      final String skipTerm = "first";
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader)
          .termFilter(new TermFilter() {
            @Override
            public boolean isAccepted(
                @Nullable final TermsEnum termsEnum,
                @NotNull final BytesRef term) {
              return !skipTerm.equals(term.utf8ToString());
            }
          })
          .build();

      new LeafReaderInstanceTest() {

        @Override
        void testHasDeletions()
            throws Exception {
          Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
        }

        @Override
        void testFieldCount()
            throws Exception {
          Assert.assertEquals("Field count mismatch.",
              idx.flds.size(), fReader.getFields().size());
        }

        @Override
        void testFieldNames()
            throws Exception {
          Assert.assertTrue("Visible field not found.",
              fReader.getFields().containsAll(idx.flds));
        }

        @Override
        void testTotalTermFreq()
            throws Exception {
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible term.",
              idx.docs, fReader.totalTermFreq(new Term("f1", "field")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for missing term.",
              0L, fReader.totalTermFreq(new Term("f1", "foo")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden term.",
              0L, fReader.totalTermFreq(new Term("f1", "first")));
        }

        @Override
        void testSumTotalTermFreq()
            throws Exception {
          Assert.assertEquals("SumTotalTermFreq mismatch for visible term.",
              14L, fReader.getSumTotalTermFreq("f1"));
        }

        @Override
        void testDocCount()
            throws Exception {
          Assert.assertEquals("Doc count mismatch.",
              idx.docs, fReader.getDocCount("f1"));
        }

        @SuppressWarnings("ObjectAllocationInLoop")
        @Override
        void testDocFreq()
            throws Exception {
          for (final String f : idx.flds) {
            Assert.assertEquals("Missing term from all documents.",
                idx.docs, fReader.docFreq(new Term(f, "value")));
            Assert.assertEquals("Found hidden term.",
                0L, fReader.docFreq(new Term(f, "first")));
          }
        }

        @Override
        void testSumDocFreq()
            throws Exception {
          Assert.assertEquals("SumDocFreq mismatch for visible term.",
              14L, fReader.getSumDocFreq("f1"));
        }

        @Override
        void testTermVectors()
            throws Exception {
          final BytesRef term = new BytesRef("first");
          for (int i = 0; i < idx.docs - 1; i++) {
            final Fields f = fReader.getTermVectors(i);
            Assert.assertEquals("Too much fields retrieved from TermVector.",
                1L, f.size());
            final TermsEnum te = f.terms("f1").iterator(null);
            Assert.assertFalse("Hidden term found.",
                te.seekExact(term));
          }
        }

        @Override
        void testNumDocs()
            throws Exception {
          Assert.assertEquals("NumDocs mismatch.", idx.docs, fReader.numDocs());
        }

        @Override
        void testMaxDoc()
            throws Exception {
          Assert.assertEquals("MaxDoc mismatch.", idx.docs, fReader.maxDoc());
        }
      };
    }
  }

  /**
   * Filter out all terms.
   *
   * @throws Exception
   */
  @SuppressWarnings({"AnonymousInnerClassMayBeStatic",
      "ImplicitNumericConversion"})
  @Test
  public void testBuilder_termFilter_allTerms()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.PLAIN)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader)
          .termFilter(new TermFilter() {
            @Override
            public boolean isAccepted(
                @Nullable final TermsEnum termsEnum,
                @NotNull final BytesRef term) {
              return false; // skip all terms
            }
          })
          .build();

      new LeafReaderInstanceTest() {

        @Override
        void testHasDeletions()
            throws Exception {
          Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
        }

        @Override
        void testFieldCount()
            throws Exception {
          Assert.assertEquals("Field count mismatch.",
              0L, fReader.getFields().size());
        }

        @Override
        void testFieldNames()
            throws Exception {
          Assert.assertTrue("Field found.", fReader.getFields().isEmpty());
        }

        @Override
        void testTotalTermFreq()
            throws Exception {
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden term.",
              0L, fReader.totalTermFreq(new Term("f1", "field")));
        }

        @Override
        void testSumTotalTermFreq()
            throws Exception {
          Assert.assertEquals("SumTotalTermFreq mismatch for hidden term.",
              0L, fReader.getSumTotalTermFreq("f1"));
        }

        @Override
        void testDocCount()
            throws Exception {
          Assert.assertEquals("Doc count mismatch.",
              0L, fReader.getDocCount("f1"));
        }

        @SuppressWarnings("ObjectAllocationInLoop")
        @Override
        void testDocFreq()
            throws Exception {
          for (final String f : idx.flds) {
            Assert.assertEquals("Missing term from all documents.",
                0L, fReader.docFreq(new Term(f, "value")));
            Assert.assertEquals("Found hidden term.",
                0L, fReader.docFreq(new Term(f, "first")));
          }
        }

        @Override
        void testSumDocFreq()
            throws Exception {
          Assert.assertEquals("SumDocFreq mismatch for visible term.",
              0L, fReader.getSumDocFreq("f1"));
        }

        @Override
        void testTermVectors()
            throws Exception {
          for (int i = 0; i < idx.docs - 1; i++) {
            final Fields f = fReader.getTermVectors(i);
            if (f != null) {
              final Terms t = f.terms("f1");
              if (t != null) {
                final TermsEnum te = t.iterator(null);
                Assert.assertNull("Term found. Expected none.", te.next());
              }
            }
          }
        }

        @Override
        void testNumDocs()
            throws Exception {
          Assert.assertEquals("NumDocs mismatch.", idx.docs, fReader.numDocs());
        }

        @Override
        void testMaxDoc()
            throws Exception {
          Assert.assertEquals("MaxDoc mismatch.", idx.docs, fReader.maxDoc());
        }
      };
    }
  }

  /**
   * Test basic {@link Filter} usage.
   *
   * @throws Exception
   */
  @SuppressWarnings({"AnonymousInnerClassMayBeStatic",
      "ImplicitNumericConversion"})
  @Test
  public void testBuilder_filter()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.PLAIN)) {
      final Query q = new TermQuery(new Term("f1", "first"));
      final Filter f = new QueryWrapperFilter(q);
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader)
          .queryFilter(f)
          .build();

      new LeafReaderInstanceTest() {

        @Override
        void testHasDeletions()
            throws Exception {
          Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
        }

        @Override
        void testFieldCount()
            throws Exception {
          Assert.assertEquals("Field count mismatch.",
              idx.flds.size(), fReader.getFields().size());
        }

        @Override
        void testFieldNames()
            throws Exception {
          Assert.assertTrue("Visible field not found.",
              fReader.getFields().containsAll(idx.flds));
        }

        @Override
        void testTotalTermFreq()
            throws Exception {
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible term.",
              1L, fReader.totalTermFreq(new Term("f1", "field")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for missing term.",
              0L, fReader.totalTermFreq(new Term("f1", "foo")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for query term.",
              1L, fReader.totalTermFreq(new Term("f1", "first")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden term.",
              0L, fReader.totalTermFreq(new Term("f1", "second")));
        }

        @Override
        void testSumTotalTermFreq()
            throws Exception {
          Assert.assertEquals("SumTotalTermFreq mismatch for visible terms.",
              3L, fReader.getSumTotalTermFreq("f1"));
        }

        @Override
        void testDocCount()
            throws Exception {
          Assert.assertEquals("Doc count mismatch.",
              1L, fReader.getDocCount("f1"));
        }

        @SuppressWarnings("ObjectAllocationInLoop")
        @Override
        void testDocFreq()
            throws Exception {
          for (final String fld : idx.flds) {
            Assert.assertEquals("Missing term from all documents.",
                1L, fReader.docFreq(new Term(fld, "value")));
            Assert.assertEquals("Found hidden term.",
                0L, fReader.docFreq(new Term(fld, "second")));
          }
        }

        @Override
        void testSumDocFreq()
            throws Exception {
          Assert.assertEquals("SumDocFreq mismatch for visible term.",
              3L, fReader.getSumDocFreq("f1"));
        }

        @Override
        void testTermVectors()
            throws Exception {
          final Fields fld = fReader.getTermVectors(0);
          Assert.assertEquals("Too much fields retrieved from TermVector.",
              1L, fld.size());
        }

        @Override
        void testNumDocs()
            throws Exception {
          Assert.assertEquals("NumDocs mismatch.", 1L, fReader.numDocs());
        }

        @Override
        void testMaxDoc()
            throws Exception {
          Assert.assertEquals("MaxDoc mismatch.", 1L, fReader.maxDoc());
        }
      };
    }
  }

  /**
   * Test {@link Filter} usage in combination with field restriction.
   *
   * @throws Exception
   */
  @SuppressWarnings({"AnonymousInnerClassMayBeStatic",
      "ImplicitNumericConversion"})
  @Test
  public void testBuilder_filter_and_field()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.ALL_FIELDS)) {
      final Query q = new TermQuery(new Term("f1", "document2field1"));
      final Filter f = new QueryWrapperFilter(q);
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader)
          .queryFilter(f)
          .fields(Collections.singleton("f2"))
          .build();

      new LeafReaderInstanceTest() {

        @Override
        void testHasDeletions()
            throws Exception {
          Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
        }

        @Override
        void testFieldCount()
            throws Exception {
          Assert.assertEquals("Field count mismatch.",
              1L, fReader.getFields().size());
        }

        @Override
        void testFieldNames()
            throws Exception {
          Assert.assertTrue("Visible field not found.",
              fReader.getFields().contains("f2"));
        }

        @Override
        void testTotalTermFreq()
            throws Exception {
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible term.",
              1L, fReader.totalTermFreq(new Term("f2", "field2")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden term.",
              0L, fReader.totalTermFreq(new Term("f1", "field1")));
        }

        @Override
        void testSumTotalTermFreq()
            throws Exception {
          Assert.assertEquals("SumTotalTermFreq mismatch for visible terms.",
              6L, fReader.getSumTotalTermFreq("f2"));
        }

        @Override
        void testDocCount()
            throws Exception {
          Assert.assertEquals("Doc count mismatch.",
              1L, fReader.getDocCount("f2"));
        }

        @SuppressWarnings("ObjectAllocationInLoop")
        @Override
        void testDocFreq()
            throws Exception {
          Assert.assertEquals("Missing term from visible document.",
              1L, fReader.docFreq(new Term("f2", "value")));
        }

        @Override
        void testSumDocFreq()
            throws Exception {
          Assert.assertEquals("SumDocFreq mismatch for visible term.",
              6L, fReader.getSumDocFreq("f2"));
        }

        @Override
        void testTermVectors()
            throws Exception {
          boolean match = false;
          for (int i = 0; i < fReader.maxDoc(); i++) {
            final Fields fld = fReader.getTermVectors(i);
            if (fld != null) {
              match = true;
              Assert.assertEquals("Too much fields retrieved from TermVector.",
                  1L, fld.size());
            }
          }
          Assert.assertTrue("Fields not found.", match);
        }

        @Override
        void testNumDocs()
            throws Exception {
          Assert.assertEquals("NumDocs mismatch.", 1L, fReader.numDocs());
        }

        @Override
        void testMaxDoc()
            throws Exception {
          Assert.assertEquals("MaxDoc mismatch.", 2L, fReader.maxDoc());
        }
      };
    }
  }

  /**
   * Test {@link Filter} usage in combination with field restriction using both
   * the same field. The excluded field is the same the query-filter will run
   * on.
   *
   * @throws Exception
   */
  @SuppressWarnings({"AnonymousInnerClassMayBeStatic",
      "ImplicitNumericConversion"})
  @Test
  public void testBuilder_filter_and_field_sameField()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.ALL_FIELDS)) {
      final Query q = new TermQuery(new Term("f2", "document2field2"));
      final Filter f = new QueryWrapperFilter(q);
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader)
          .queryFilter(f)
          .fields(Collections.singleton("f2"), true)
          .build();

      new LeafReaderInstanceTest() {

        @Override
        void testHasDeletions()
            throws Exception {
          Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
        }

        @Override
        void testFieldCount()
            throws Exception {
          Assert.assertEquals("Field count mismatch.",
              idx.flds.size() - 1, fReader.getFields().size());
        }

        @Override
        void testFieldNames()
            throws Exception {
          Assert.assertFalse("Excluded field found.",
              fReader.getFields().contains("f2"));
        }

        @Override
        void testTotalTermFreq()
            throws Exception {
          // visible fields
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & term.",
              1L, fReader.totalTermFreq(new Term("f1", "field1")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & term.",
              1L, fReader.totalTermFreq(new Term("f3", "field3")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible field & missing term.",
              0L, fReader.totalTermFreq(new Term("f3", "foo")));
          // hidden fields
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden field & term.",
              0L, fReader.totalTermFreq(new Term("f2", "field2")));
        }

        @Override
        void testSumTotalTermFreq()
            throws Exception {
          Assert.assertEquals("SumTotalTermFreq mismatch for visible field.",
              6L, fReader.getSumTotalTermFreq("f1"));
          Assert.assertEquals("SumTotalTermFreq mismatch for visible field.",
              6L, fReader.getSumTotalTermFreq("f3"));
          Assert.assertEquals("SumTotalTermFreq mismatch for hidden field.",
              0L, fReader.getSumTotalTermFreq("f2"));
        }

        @Override
        void testDocCount()
            throws Exception {
          Assert.assertEquals("Doc count mismatch for visible field.",
              1L, fReader.getDocCount("f1"));
          Assert.assertEquals("Doc count mismatch for visible field.",
              1L, fReader.getDocCount("f3"));
          // check visibility of all docs without visible fields
          Assert.assertEquals("Document visible with hidden field.",
              0L, fReader.getDocCount("f2"));
        }

        @Override
        void testDocFreq()
            throws Exception {
          Assert.assertEquals("Missing term from all documents.",
              1L, fReader.docFreq(new Term("f1", "field1")));
          Assert.assertEquals("Found term from hidden documents.",
              0L, fReader.docFreq(new Term("f2", "field2")));
          Assert.assertEquals("Missing term from all documents.",
              1L, fReader.docFreq(new Term("f3", "field3")));
        }

        @Override
        void testSumDocFreq()
            throws Exception {
          Assert.assertEquals("SumDocFreq mismatch for visible field.",
              6L, fReader.getSumDocFreq("f1"));
          Assert.assertEquals("SumDocFreq mismatch for visible field.",
              6L, fReader.getSumDocFreq("f3"));
          Assert.assertEquals("SumDocFreq has result for hidden field.",
              0L, fReader.getSumDocFreq("f2"));
        }

        @Override
        void testTermVectors()
            throws Exception {
          for (int i = 0; i < idx.docs - 1; i++) {
            final Fields flds = fReader.getTermVectors(i);
            Assert.assertEquals("Too much fields retrieved from TermVector.",
                2L, flds.size());
            flds.forEach(fld ->
                    Assert.assertFalse("Hidden field found.", "f2".equals(fld))
            );
          }
        }

        @Override
        void testNumDocs()
            throws Exception {
          Assert.assertEquals("NumDocs mismatch.",
              1L, fReader.numDocs());
        }

        @Override
        void testMaxDoc()
            throws Exception {
          Assert.assertEquals("MaxDoc mismatch.",
              2L, fReader.maxDoc());
        }
      };
    }
  }

  /**
   * Test {@link Filter} usage in combination with {@link TermFilter}
   * restriction.
   *
   * @throws Exception
   */
  @SuppressWarnings({"AnonymousInnerClassMayBeStatic",
      "ImplicitNumericConversion"})
  @Test
  public void testBuilder_filter_and_termFilter()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.ALL_FIELDS)) {
      final String skipTerm = "document2field3";
      final Query q = new TermQuery(new Term("f1", "document2field1"));
      final Filter f = new QueryWrapperFilter(q);
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader)
          .queryFilter(f)
          .termFilter(new TermFilter() {
            @Override
            public boolean isAccepted(
                @Nullable final TermsEnum termsEnum,
                @NotNull final BytesRef term) {
              return !skipTerm.equals(term.utf8ToString());
            }
          })
          .build();

      new LeafReaderInstanceTest() {

        @Override
        void testHasDeletions()
            throws Exception {
          Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
        }

        @Override
        void testFieldCount()
            throws Exception {
          Assert.assertEquals("Field count mismatch.",
              3L, fReader.getFields().size());
        }

        @Override
        void testFieldNames()
            throws Exception {
          for (final String fld : idx.flds) {
            Assert.assertTrue("Visible field not found.",
                fReader.getFields().contains(fld));
          }
        }

        @Override
        void testTotalTermFreq()
            throws Exception {
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible term.",
              1L, fReader.totalTermFreq(new Term("f1", "field1")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible term.",
              1L, fReader.totalTermFreq(new Term("f2", "field2")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for visible term.",
              1L, fReader.totalTermFreq(new Term("f3", "field3")));
          Assert.assertEquals(
              "TotalTermFreq mismatch for hidden term.",
              0L, fReader.totalTermFreq(new Term("f3", "document2field3")));
        }

        @Override
        void testSumTotalTermFreq()
            throws Exception {
          Assert.assertEquals("SumTotalTermFreq mismatch for visible terms.",
              6L, fReader.getSumTotalTermFreq("f2"));
        }

        @Override
        void testDocCount()
            throws Exception {
          for (final String fld : idx.flds) {
            Assert.assertEquals("Doc count mismatch.",
                1L, fReader.getDocCount(fld));
          }
        }

        @SuppressWarnings("ObjectAllocationInLoop")
        @Override
        void testDocFreq()
            throws Exception {
          Assert.assertEquals("Missing term from visible document.",
              1L, fReader.docFreq(new Term("f2", "value")));
          Assert.assertEquals("Hidden term found.",
              0L, fReader.docFreq(new Term("f1", "document1field1")));
          Assert.assertEquals("Hidden term found.",
              0L, fReader.docFreq(new Term("f3", "document2field3")));
        }

        @Override
        void testSumDocFreq()
            throws Exception {
          Assert.assertEquals("SumDocFreq mismatch for visible term.",
              6L, fReader.getSumDocFreq("f2"));
          Assert.assertEquals("SumDocFreq mismatch for visible term.",
              5L, fReader.getSumDocFreq("f3"));
        }

        @Override
        void testTermVectors()
            throws Exception {
          boolean match = false;
          final BytesRef term = new BytesRef(skipTerm);
          for (int i = 0; i < fReader.maxDoc(); i++) {
            final Fields fld = fReader.getTermVectors(i);
            if (fld != null) {
              match = true;
              Assert.assertEquals(
                  "Number of fields retrieved from TermVector do not match.",
                  3L, fld.size());
              final Terms t = fld.terms("f3");
              if (t != null) {
                final TermsEnum te = t.iterator(null);
                Assert.assertFalse("Hidden term found.", te.seekExact(term));
              }
            }
          }
          Assert.assertTrue("Fields not found.", match);
        }

        @Override
        void testNumDocs()
            throws Exception {
          Assert.assertEquals("NumDocs mismatch.", 1L, fReader.numDocs());
        }

        @Override
        void testMaxDoc()
            throws Exception {
          Assert.assertEquals("MaxDoc mismatch.", 2L, fReader.maxDoc());
        }
      };
    }
  }

  @Test
  public void testUnwrap()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.PLAIN)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader).build();

      Assert.assertFalse("Unwrap failed.",
          FilteredDirectoryReader.class.isInstance(fReader.unwrap()));
    }
  }

  @Test
  public void testDoWrapDirectoryReader()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.PLAIN)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader).build();

      final FilteredDirectoryReader result = fReader.doWrapDirectoryReader
          (reader);

      Assert.assertTrue("Wrap failed.",
          FilteredDirectoryReader.class.isInstance(result));
    }
  }

  @Test
  public void testHasDeletions()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.PLAIN)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader).build();

      Assert.assertFalse("Reader has deletions.", fReader.hasDeletions());
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testGetFields()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.PLAIN)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader).build();

      Assert.assertEquals("Field count mismatch.",
          idx.flds.size(), fReader.getFields().size());
      Assert.assertTrue("Field list mismatch.",
          fReader.getFields().containsAll(idx.flds));
    }
  }

  @Test
  public void testGetSubReaders()
      throws Exception {
    try (TestMemIndex idx = new TestMemIndex(Index.PLAIN)) {
      final DirectoryReader reader = DirectoryReader.open(idx.dir);
      final FilteredDirectoryReader fReader = new Builder(reader).build();

      Assert.assertFalse("No subReaders.", fReader.getSubReaders().isEmpty());
    }
  }

  /**
   * Class gathering all instance test methods to ensure no important method
   * gets missed.
   */
  private abstract static class LeafReaderInstanceTest {
    @SuppressWarnings({"AbstractMethodCallInConstructor",
        "OverridableMethodCallDuringObjectConstruction",
        "OverriddenMethodCallDuringObjectConstruction"})
    LeafReaderInstanceTest()
        throws Exception {
      testHasDeletions();
      testFieldCount();
      testFieldNames();
      testTotalTermFreq();
      testSumTotalTermFreq();
      testDocCount();
      testDocFreq();
      testSumDocFreq();
      testTermVectors();
      testNumDocs();
      testMaxDoc();
    }

    abstract void testHasDeletions()
        throws Exception;

    abstract void testFieldCount()
        throws Exception;

    abstract void testFieldNames()
        throws Exception;

    abstract void testTotalTermFreq()
        throws Exception;

    abstract void testSumTotalTermFreq()
        throws Exception;

    abstract void testDocCount()
        throws Exception;

    abstract void testDocFreq()
        throws Exception;

    abstract void testSumDocFreq()
        throws Exception;

    abstract void testTermVectors()
        throws Exception;

    abstract void testNumDocs()
        throws Exception;

    abstract void testMaxDoc()
        throws Exception;
  }

  /**
   * Simple static memory index for testing.
   *
   * @author Jens Bertram (code@jens-bertram.net)
   */
  @SuppressWarnings("JavaDoc")
  static final class TestMemIndex
      implements AutoCloseable {
    final Directory dir;
    /**
     * Document fields.
     */
    List<String> flds;
    /**
     * Number of document.
     */
    int docs;

    @SuppressWarnings("resource")
    TestMemIndex(final Index idxType)
        throws IOException {
      this.dir = new RAMDirectory();
      final IndexWriter wrtr = new IndexWriter(this.dir,
          new IndexWriterConfig(new WhiteSpaceAnalyzer()));
      switch (idxType) {
        case ALL_FIELDS:
          wrtr.addDocuments(getAllFieldsIndexDocs());
          break;
        case NO_TVECTORS:
          wrtr.addDocuments(getNoTVIndexDocs());
          break;
        case PLAIN:
          wrtr.addDocuments(getPlainIndexDocs());
          break;
      }
      wrtr.close();
    }

    Iterable<Document> getAllFieldsIndexDocs() {
      this.flds = Arrays.asList("f1", "f2", "f3");

      final Collection<Document> docs = new ArrayList<>(3);

      final Document doc1 = new Document();
      doc1.add(new VecTextField("f1",
          "first field value document1 field1 document1field1", Store.NO));
      doc1.add(new VecTextField("f2",
          "second field value document1 field2 document1field2", Store.NO));
      doc1.add(new VecTextField("f3",
          "third field value document1 field3 document1field3", Store.NO));
      docs.add(doc1);

      final Document doc2 = new Document();
      doc2.add(new VecTextField("f1",
          "first field value document2 field1 document2field1", Store.NO));
      doc2.add(new VecTextField("f2",
          "second field value document2 field2 document2field2", Store.NO));
      doc2.add(new VecTextField("f3",
          "third field value document2 field3 document2field3", Store.NO));
      docs.add(doc2);

      final Document doc3 = new Document();
      doc3.add(new VecTextField("f1",
          "first field value document3 field1 document3field1", Store.NO));
      doc3.add(new VecTextField("f2",
          "second field value document3 field2 document3field2", Store.NO));
      doc3.add(new VecTextField("f3",
          "third field value document3 field3 document3field3", Store.NO));
      docs.add(doc3);

      this.docs = docs.size();
      return docs;
    }

    Iterable<Document> getNoTVIndexDocs() {
      this.flds = Arrays.asList("f1", "f2", "f3");

      final Collection<Document> docs = new ArrayList<>(1);

      final Document doc1 = new Document();
      doc1.add(new TextField("f1",
          "first field value document1 field1 document1field1", Store.NO));
      doc1.add(new TextField("f2",
          "second field value document1 field2 document1field2", Store.NO));
      doc1.add(new TextField("f3",
          "third field value document1 field3 document1field3", Store.NO));
      docs.add(doc1);

      this.docs = docs.size();
      return docs;
    }

    Iterable<Document> getPlainIndexDocs() {
      this.flds = Collections.singletonList("f1");

      final Collection<Document> docs = new ArrayList<>(5);

      final Document doc1 = new Document();
      doc1.add(new VecTextField("f1", "first field value", Store.NO));
      docs.add(doc1);

      final Document doc2 = new Document();
      doc2.add(new VecTextField("f1", "second field value", Store.NO));
      docs.add(doc2);

      final Document doc3 = new Document();
      doc3.add(new VecTextField("f1", "third field value", Store.NO));
      docs.add(doc3);

      final Document doc4 = new Document();
      doc4.add(new VecTextField("f1", "fourth field value", Store.NO));
      docs.add(doc4);

      final Document doc5 = new Document();
      doc5.add(new VecTextField("f1", "fifth field value", Store.NO));
      docs.add(doc5);

      this.docs = docs.size();
      return docs;
    }

    @Override
    public void close()
        throws Exception {
      this.dir.close();
    }

    enum Index {
      PLAIN, ALL_FIELDS, NO_TVECTORS
    }

    private static final class WhiteSpaceAnalyzer
        extends Analyzer {

      WhiteSpaceAnalyzer() {
      }

      @Override
      protected TokenStreamComponents createComponents(final String fieldName) {
        return new TokenStreamComponents(new WhitespaceTokenizer());
      }
    }
  }
}