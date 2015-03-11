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
import de.unihildesheim.iw.lucene.EmptyBitSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link BitsUtils}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class BitsUtilsTest
    extends TestCase {
  public BitsUtilsTest() {
    super(LoggerFactory.getLogger(BitsUtilsTest.class));
  }

  /**
   * An empty {@link BitSet} implementation without any functionality.
   */
  private static final Bits EMPTY_BITSET = new EmptyBitSet();

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testBits2BitSet()
      throws Exception {
    final FixedBitSet fbs = new FixedBitSet(11);
    fbs.set(1);
    fbs.set(3);
    fbs.set(6);
    fbs.set(7);
    fbs.set(8);
    fbs.set(10);

    final BitSet result = BitsUtils.bits2BitSet(fbs);

    Assert.assertEquals("Bit count mismatch.",
        fbs.cardinality(), result.cardinality());
    for (int i = 0; i < 11; i++) {
      Assert.assertEquals("Bits mismatch.", fbs.get(i), result.get(i));
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testBits2BitSet_empty()
      throws Exception {
    Assert.assertEquals("Too much bits streamed.",
        0L, BitsUtils.bits2BitSet(EMPTY_BITSET).cardinality());
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testBits2BitSet_null()
      throws Exception {
    final BitSet result = BitsUtils.bits2BitSet(null);
    Assert.assertNull("Expected result to be null.", result);
  }

  @Test
  public void testBits2FixedBitSet()
      throws Exception {
    final FixedBitSet fbs = new FixedBitSet(11);
    fbs.set(1);
    fbs.set(3);
    fbs.set(6);
    fbs.set(7);
    fbs.set(8);
    fbs.set(10);

    final FixedBitSet result = BitsUtils.bits2FixedBitSet(fbs);
    Assert.assertTrue("BitSets not equal.", fbs.equals(result));
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testBits2FixedBitSet_empty()
      throws Exception {
    Assert.assertEquals("Too much bits streamed.",
        0L, BitsUtils.bits2FixedBitSet(EMPTY_BITSET).cardinality());
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testBits2FixedBitSet_null()
      throws Exception {
    final BitSet result = BitsUtils.bits2FixedBitSet(null);
    Assert.assertNull("Expected result to be null.", result);
  }

  @Test
  public void testArrayToBits()
      throws Exception {
    final FixedBitSet fbs = new FixedBitSet(11);
    fbs.set(1);
    fbs.set(3);
    fbs.set(6);
    fbs.set(7);
    fbs.set(8);
    fbs.set(10);
    final int[] bits = {1, 3, 6, 7, 8, 10};

    final FixedBitSet result = BitsUtils.arrayToBits(bits);
    Assert.assertTrue("BitSets not equal.", fbs.equals(result));
  }

  @SuppressWarnings({"ZeroLengthArrayAllocation", "RedundantArrayCreation"})
  @Test
  public void testArrayToBits_empty()
      throws Exception {
    final FixedBitSet fbs = new FixedBitSet(0);
    final FixedBitSet result = BitsUtils.arrayToBits(new int[]{});
    Assert.assertTrue("BitSets not equal.", fbs.equals(result));
  }
}