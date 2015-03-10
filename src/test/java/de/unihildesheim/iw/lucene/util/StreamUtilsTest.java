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

package de.unihildesheim.iw.lucene.util;

import de.unihildesheim.iw.TestCase;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet.Builder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Test for {@link StreamUtils}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class StreamUtilsTest
    extends TestCase {
  public StreamUtilsTest() {
    super(LoggerFactory.getLogger(StreamUtilsTest.class));
  }

  @Test
  public void testStream_bytesRefArray()
      throws Exception {
    final BytesRefArray bArr = new BytesRefArray(Counter.newCounter(false));
    bArr.append(new BytesRef("foo"));
    bArr.append(new BytesRef("bar"));
    bArr.append(new BytesRef("baz"));

    Assert.assertEquals("Not all items streamed.",
        3L, StreamUtils.stream(bArr).count());

    Assert.assertEquals("Term not found.", 1L,
        StreamUtils.stream(bArr)
            .filter(br -> br.bytesEquals(new BytesRef("foo"))).count());
    Assert.assertEquals("Term not found.", 1L,
        StreamUtils.stream(bArr)
            .filter(br -> br.bytesEquals(new BytesRef("bar"))).count());
    Assert.assertEquals("Term not found.", 1L,
        StreamUtils.stream(bArr)
            .filter(br -> br.bytesEquals(new BytesRef("baz"))).count());

    Assert.assertEquals("Unknown term found.", 0L,
        StreamUtils.stream(bArr)
            .filter(t ->
                    !t.bytesEquals(new BytesRef("foo")) &&
                        !t.bytesEquals(new BytesRef("bar")) &&
                        !t.bytesEquals(new BytesRef("baz"))
            ).count());
  }

  @Test
  public void testStream_bytesRefArray_nonUnique()
      throws Exception {
    final BytesRefArray bArr = new BytesRefArray(Counter.newCounter(false));
    bArr.append(new BytesRef("foo"));
    bArr.append(new BytesRef("bar"));
    bArr.append(new BytesRef("foo"));
    bArr.append(new BytesRef("baz"));
    bArr.append(new BytesRef("bar"));
    bArr.append(new BytesRef("foo"));

    Assert.assertEquals("Not all items streamed.",
        6L, StreamUtils.stream(bArr).count());

    Assert.assertEquals("Term count mismatch.", 3L,
        StreamUtils.stream(bArr)
            .filter(br -> br.bytesEquals(new BytesRef("foo"))).count());
    Assert.assertEquals("Term count mismatch.", 2L,
        StreamUtils.stream(bArr)
            .filter(br -> br.bytesEquals(new BytesRef("bar"))).count());
    Assert.assertEquals("Term not found.", 1L,
        StreamUtils.stream(bArr)
            .filter(br -> br.bytesEquals(new BytesRef("baz"))).count());

    Assert.assertEquals("Unknown term found.", 0L,
        StreamUtils.stream(bArr)
            .filter(t ->
                    !t.bytesEquals(new BytesRef("foo")) &&
                        !t.bytesEquals(new BytesRef("bar")) &&
                        !t.bytesEquals(new BytesRef("baz"))
            ).count());
  }

  @Test
  public void testStream_bytesRefArray_empty()
      throws Exception {
    final BytesRefArray bArr = new BytesRefArray(Counter.newCounter(false));
    Assert.assertEquals("Too much items streamed.",
        0L, StreamUtils.stream(bArr).count());
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testStream_bytesRefArray_null()
      throws Exception {
    try {
      StreamUtils.stream((BytesRefArray) null);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testStream_docIdSet()
      throws Exception {
    final Builder disBuilder = new Builder(10);
    disBuilder
        .add(1)
        .add(3)
        .add(6)
        .add(7)
        .add(8)
        .add(10);
    final DocIdSet dis = disBuilder.build();

    Assert.assertEquals("Not all document ids streamed.",
        6L, StreamUtils.stream(dis).count());

    Assert.assertEquals("Document id count mismatch.", 1L,
        StreamUtils.stream(dis).filter(id -> id == 1).count());
    Assert.assertEquals("Document id count mismatch.", 1L,
        StreamUtils.stream(dis).filter(id -> id == 3).count());
    Assert.assertEquals("Document id count mismatch.", 1L,
        StreamUtils.stream(dis).filter(id -> id == 6).count());
    Assert.assertEquals("Document id count mismatch.", 1L,
        StreamUtils.stream(dis).filter(id -> id == 7).count());
    Assert.assertEquals("Document id count mismatch.", 1L,
        StreamUtils.stream(dis).filter(id -> id == 8).count());
    Assert.assertEquals("Document id count mismatch.", 1L,
        StreamUtils.stream(dis).filter(id -> id == 10).count());

    Assert.assertEquals("Unknown document id found.", 0L,
        StreamUtils.stream(dis).filter(id ->
                id != 1 && id != 3 && id != 6 && id != 7 && id != 8 && id != 10
        ).count());
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testStream_docIdSet_null()
      throws Exception {
    try {
      StreamUtils.stream((DocIdSet) null);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testStream_docIdSet_empty()
      throws Exception {
    final Builder disBuilder = new Builder(10);
    final DocIdSet dis = disBuilder.build();

    Assert.assertEquals("Too much document ids streamed.",
        0L, StreamUtils.stream(dis).count());
  }

  @Test
  public void testStream_docIdSetIterator()
      throws Exception {
    final Builder disBuilder = new Builder(10);
    disBuilder
        .add(1)
        .add(3)
        .add(6)
        .add(7)
        .add(8)
        .add(10);
    final DocIdSet dis = disBuilder.build();

    Assert.assertEquals("Not all document ids streamed.",
        6L, StreamUtils.stream(dis.iterator()).count());

    Assert.assertEquals("Document id count mismatch.", 1L,
        StreamUtils.stream(dis.iterator()).filter(id -> id == 1).count());
    Assert.assertEquals("Document id count mismatch.", 1L,
        StreamUtils.stream(dis.iterator()).filter(id -> id == 3).count());
    Assert.assertEquals("Document id count mismatch.", 1L,
        StreamUtils.stream(dis.iterator()).filter(id -> id == 6).count());
    Assert.assertEquals("Document id count mismatch.", 1L,
        StreamUtils.stream(dis.iterator()).filter(id -> id == 7).count());
    Assert.assertEquals("Document id count mismatch.", 1L,
        StreamUtils.stream(dis.iterator()).filter(id -> id == 8).count());
    Assert.assertEquals("Document id count mismatch.", 1L,
        StreamUtils.stream(dis.iterator()).filter(id -> id == 10).count());

    Assert.assertEquals("Unknown document id found.", 0L,
        StreamUtils.stream(dis.iterator()).filter(id ->
                id != 1 && id != 3 && id != 6 && id != 7 && id != 8 && id != 10
        ).count());
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testStream_docIdSetIterator_null()
      throws Exception {
    // expect an empty stream
    Assert.assertEquals("Too much document ids streamed.",
        0L, StreamUtils.stream((DocIdSetIterator) null).count());
  }

  @Test
  public void testStream_docIdSetIterator_empty()
      throws Exception {
    final Builder disBuilder = new Builder(10);
    final DocIdSet dis = disBuilder.build();

    Assert.assertEquals("Too much document ids streamed.",
        0L, StreamUtils.stream(dis.iterator()).count());
  }

  @Test
  public void testStream_bitSet()
      throws Exception {
    final FixedBitSet bits = new FixedBitSet(11);
    bits.set(1);
    bits.set(3);
    bits.set(6);
    bits.set(7);
    bits.set(8);
    bits.set(10);

    Assert.assertEquals("Not all bits streamed.",
        6L, StreamUtils.stream(bits).count());

    Assert.assertEquals("Bit not found.", 1L,
        StreamUtils.stream(bits).filter(id -> id == 1).count());
    Assert.assertEquals("Bit not found.", 1L,
        StreamUtils.stream(bits).filter(id -> id == 3).count());
    Assert.assertEquals("Bit not found.", 1L,
        StreamUtils.stream(bits).filter(id -> id == 6).count());
    Assert.assertEquals("Bit not found.", 1L,
        StreamUtils.stream(bits).filter(id -> id == 7).count());
    Assert.assertEquals("Bit not found.", 1L,
        StreamUtils.stream(bits).filter(id -> id == 8).count());
    Assert.assertEquals("Bit not found.", 1L,
        StreamUtils.stream(bits).filter(id -> id == 10).count());

    Assert.assertEquals("Unknown document id found.", 0L,
        StreamUtils.stream(bits).filter(id ->
                id != 1 && id != 3 && id != 6 && id != 7 && id != 8 && id != 10
        ).count());
  }

  @Test
  public void testStream_bitSet_empty()
      throws Exception {
    @SuppressWarnings("AnonymousInnerClassMayBeStatic")
    final BitSet bits = new BitSet() {
      @Override
      public void set(final int i) {
        // NOP
      }

      @Override
      public void clear(final int startIndex, final int endIndex) {
        // NOP
      }

      @Override
      public int cardinality() {
        return 0;
      }

      @Override
      public int prevSetBit(final int index) {
        return 0;
      }

      @Override
      public int nextSetBit(final int index) {
        return 0;
      }

      @Override
      public long ramBytesUsed() {
        return 0L;
      }

      @Override
      public void clear(final int index) {
        // NOP
      }

      @Override
      public boolean get(final int index) {
        return false;
      }

      @Override
      public int length() {
        return 0;
      }
    };

    Assert.assertEquals("Too much bits streamed.",
        0L, StreamUtils.stream(bits).count());
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testStream_bitSet_null()
      throws Exception {
    try {
      StreamUtils.stream((BitSet) null);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testStream_termsEnum()
      throws Exception {
    final BytesRef[] terms = {
        new BytesRef("foo"),
        new BytesRef("bar"),
        new BytesRef("baz")
    };

    final class TEnum
        extends TermsEnum {
      int idx = 0;

      @Override
      public SeekStatus seekCeil(final BytesRef text) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void seekExact(final long ord) {
        throw new UnsupportedOperationException();
      }

      @Override
      public BytesRef term() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long ord() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int docFreq() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long totalTermFreq() {
        throw new UnsupportedOperationException();
      }

      @Override
      public DocsEnum docs(final Bits liveDocs, final DocsEnum reuse,
          final int flags) {
        throw new UnsupportedOperationException();
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(final Bits liveDocs,
          final DocsAndPositionsEnum reuse, final int flags) {
        throw new UnsupportedOperationException();
      }

      @Nullable
      @Override
      public BytesRef next() {
        if (this.idx < terms.length) {
          return terms[this.idx++];
        }
        return null;
      }
    }

    Assert.assertEquals("Not all terms streamed.",
        (long) terms.length, StreamUtils.stream(new TEnum()).count());

    Assert.assertEquals("Term count mismatch.", 1L,
        StreamUtils.stream(new TEnum())
            .filter(t -> t.bytesEquals(new BytesRef("foo"))).count());
    Assert.assertEquals("Term count mismatch.", 1L,
        StreamUtils.stream(new TEnum())
            .filter(t -> t.bytesEquals(new BytesRef("bar"))).count());
    Assert.assertEquals("Term count mismatch.", 1L,
        StreamUtils.stream(new TEnum())
            .filter(t -> t.bytesEquals(new BytesRef("baz"))).count());

    Assert.assertEquals("Unknown term found.", 0L,
        StreamUtils.stream(new TEnum())
            .filter(t ->
                    !t.bytesEquals(new BytesRef("foo")) &&
                        !t.bytesEquals(new BytesRef("bar")) &&
                        !t.bytesEquals(new BytesRef("baz"))
            ).count());
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testStream_termsEnum_null()
      throws Exception {
    try {
      StreamUtils.stream((TermsEnum) null);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }
}