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
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet.Builder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DocIdSetUtils}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class DocIdSetUtilsTest
    extends TestCase {
  public DocIdSetUtilsTest() {
    super(LoggerFactory.getLogger(DocIdSetUtilsTest.class));
  }

  @Test
  public void testCardinality()
      throws Exception {
    final DocIdSet dis = new Builder(11)
        .add(1)
        .add(3)
        .add(6)
        .add(7)
        .add(8)
        .add(10).build();

    Assert.assertEquals("Cardinality mismatch",
        6L, (long) DocIdSetUtils.cardinality(dis));
  }

  @Test
  public void testCardinality_empty()
      throws Exception {
    final BitSet bits = new FixedBitSet(10);

    Assert.assertEquals("Cardinality mismatch",
        0L, (long) DocIdSetUtils.cardinality(new BitDocIdSet(bits)));
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testCardinality_null()
      throws Exception {
    try {
      DocIdSetUtils.cardinality(null);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testMaxDoc()
      throws Exception {
    final DocIdSet dis = new Builder(9)
        .add(1)
        .add(3)
        .add(6)
        .add(7)
        .add(8).build();

    Assert.assertEquals("MaxDoc mismatch",
        8L, (long) DocIdSetUtils.maxDoc(dis));
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testMaxDoc_null()
      throws Exception {
    try {
      DocIdSetUtils.maxDoc(null);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testMaxDoc_empty()
      throws Exception {
    final BitSet bits = new FixedBitSet(10);

    Assert.assertEquals("Cardinality mismatch",
        -1L, (long) DocIdSetUtils.maxDoc(new BitDocIdSet(bits)));
  }

  @Test
  public void testMaxDoc_first()
      throws Exception {
    final BitSet bits = new FixedBitSet(10);
    bits.set(0);

    Assert.assertEquals("Cardinality mismatch",
        0L, (long) DocIdSetUtils.maxDoc(new BitDocIdSet(bits)));
  }

  @Test
  public void testBits()
      throws Exception {
    final DocIdSet dis = new Builder(9)
        .add(1)
        .add(3)
        .add(6)
        .add(7)
        .add(8).build();

    final BitSet bits = DocIdSetUtils.bits(dis);
    Assert.assertNotNull("Bits were null.", bits);

    final boolean allMatched = bits.get(1) && bits.get(3) && bits.get(6) &&
        bits.get(7) && bits.get(8);
    Assert.assertTrue("Not all required bits set.", allMatched);

    final boolean failMatched = bits.get(2) || bits.get(4) || bits.get(5);
    Assert.assertFalse("Wrong bits set.", failMatched);
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testBits_null()
      throws Exception {
    try {
      DocIdSetUtils.bits(null);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testBits_empty()
      throws Exception {
    final BitSet bits = new FixedBitSet(10);

    Assert.assertEquals("Cardinality mismatch",
        0L, (long) DocIdSetUtils.bits(new BitDocIdSet(bits)).cardinality());
  }
}