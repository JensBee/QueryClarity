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

package de.unihildesheim.iw.util;

import de.unihildesheim.iw.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link MathUtils}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class MathUtilsTest
    extends TestCase {
  public MathUtilsTest() {
    super(LoggerFactory.getLogger(MathUtilsTest.class));
  }

  @Test
  public void testLog2()
      throws Exception {
    final double value = 3d;
    final double expected = Math.log(value) / Math.log(2d);
    final double result = MathUtils.log2(value);

    Assert.assertEquals("Log2 value differs.", expected, result, 0d);
  }

  @Test
  public void testLogN()
      throws Exception {
    final double value = 3d;

    for (int i = 1; i < RandomValue.getInteger(10, 100); i++) {
      final double expected = Math.log(value) / Math.log((double) i);
      final double result = MathUtils.logN((double) i, value);
      Assert.assertEquals("Log2 value differs.", expected, result, 0d);
    }
  }
}