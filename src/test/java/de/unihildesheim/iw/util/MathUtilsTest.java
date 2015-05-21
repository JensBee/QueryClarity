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
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation.ScoreTupleHighPrecision;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation.ScoreTupleLowPrecision;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

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

  @Test
  public void testklDivergence_double()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 10d),
        new ScoreTupleLowPrecision(1d, 1d),
    }; // sum: q:3d, c:14d
    // r += (qModel/sums[qModel]) * log2((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final double oneDivThree = 1d / 3d;
    final double expected =
        (oneDivThree * Math.log(oneDivThree / (3d / 14d)) +
            oneDivThree * Math.log(oneDivThree / (10d / 14d)) +
            oneDivThree * Math.log(oneDivThree / (1d / 14d))) / Math.log(2);
    final double result = MathUtils.klDivergence(dataSet);

    Assert.assertEquals("Score value differs", expected, result, 0d);
  }

  @Test
  public void testklDivergence_double_zeroValue_cModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 0d),
        new ScoreTupleLowPrecision(1d, 1d),
    }; // sum: q:2d, c:4d
    // r += (qModel/sums[qModel]) * log2((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final double oneDivTwo = 1d / 2d;
    final double expected =
        (oneDivTwo * Math.log(oneDivTwo / (3d / 4d)) +
            oneDivTwo * Math.log(oneDivTwo / (1d / 4d))) / Math.log(2);
    final double result = MathUtils.klDivergence(dataSet);

    Assert.assertEquals("Score value differs", expected, result, 0d);
  }

  @Test
  public void testklDivergence_double_zeroValue_qModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(0d, 10d),
        new ScoreTupleLowPrecision(1d, 1d),
    }; // sum: q:2d, c:4d
    // r += (qModel/sums[qModel]) * log2((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final double oneDivThree = 1d / 2d;
    final double expected =
        (oneDivThree * Math.log(oneDivThree / (3d / 4d)) +
            oneDivThree * Math.log(oneDivThree / (1d / 4d))) / Math.log(2);
    final double result = MathUtils.klDivergence(dataSet);

    Assert.assertEquals("Score value differs", expected, result, 0d);
  }

  @Test
  public void testklDivergence_double_nullElement()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        null,
        new ScoreTupleLowPrecision(1d, 1d),
    };
    try {
      MathUtils.klDivergence(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testklDivergence_double_tooLowValue_qModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 1d),
        new ScoreTupleLowPrecision(-1d, 3d),
        new ScoreTupleLowPrecision(1d, 3d),
    };
    try {
      MathUtils.klDivergence(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testklDivergence_double_tooLowValue_cModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 1d),
        new ScoreTupleLowPrecision(1d, -3d),
        new ScoreTupleLowPrecision(1d, 3d),
    };
    try {
      MathUtils.klDivergence(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testklDivergence_bigDecimal()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(10L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    }; // sum: q:3d, c:14d
    // r += (qModel/sums[qModel]) * log2((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final BigDecimal oneDivThree = BigDecimal.ONE.divide(
        BigDecimal.valueOf(3L), MathUtils.MATH_CONTEXT);
    final BigDecimal sumCModel = BigDecimal.valueOf(14L);

    final BigDecimal expected =
        oneDivThree.multiply(BigMathFunctions.ln(
            oneDivThree.divide(
                BigDecimal.valueOf(3L).divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            ), MathUtils.MATH_CONTEXT.getPrecision()
        ), MathUtils.MATH_CONTEXT).add(oneDivThree.multiply(BigMathFunctions.ln(
            oneDivThree.divide(
                BigDecimal.valueOf(10L).divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            ), MathUtils.MATH_CONTEXT.getPrecision()
        )), MathUtils.MATH_CONTEXT).add(oneDivThree.multiply(BigMathFunctions.ln(
            oneDivThree.divide(
                BigDecimal.ONE.divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            ), MathUtils.MATH_CONTEXT.getPrecision()
        )), MathUtils.MATH_CONTEXT)
            .divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);

//    final BigDecimal expected =
//        oneDivThree.multiply(BigDecimalMath.log(
//            oneDivThree.divide(
//                BigDecimal.valueOf(3L).divide(sumCModel,
//                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
//            )
//        ), MathUtils.MATH_CONTEXT).add(oneDivThree.multiply(BigDecimalMath.log(
//            oneDivThree.divide(
//                BigDecimal.valueOf(10L).divide(sumCModel,
//                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
//            )
//        )), MathUtils.MATH_CONTEXT).add(oneDivThree.multiply(BigDecimalMath.log(
//            oneDivThree.divide(
//                BigDecimal.ONE.divide(sumCModel,
//                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
//            )
//        )), MathUtils.MATH_CONTEXT)
//            .divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);

    final BigDecimal result = MathUtils.klDivergence(dataSet);

    Assert.assertEquals("Score value differs", expected, result);
  }

  @Test
  public void testklDivergence_bigDecimal_zeroValue_cModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ZERO),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    }; // sum: q:3d, c:14d
    // r += (qModel/sums[qModel]) * log2((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final BigDecimal oneDivTwo = BigDecimal.ONE.divide(
        BigDecimal.valueOf(2L), MathUtils.MATH_CONTEXT);
    final BigDecimal sumCModel = BigDecimal.valueOf(4L);

    final BigDecimal expected =
        oneDivTwo.multiply(BigMathFunctions.ln(
            oneDivTwo.divide(
                BigDecimal.valueOf(3L).divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            ), MathUtils.MATH_CONTEXT.getPrecision()
        ), MathUtils.MATH_CONTEXT).add(oneDivTwo.multiply(BigMathFunctions.ln(
            oneDivTwo.divide(
                BigDecimal.ONE.divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            ), MathUtils.MATH_CONTEXT.getPrecision()
        )), MathUtils.MATH_CONTEXT)
            .divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);

//    final BigDecimal expected =
//        oneDivTwo.multiply(BigDecimalMath.log(
//            oneDivTwo.divide(
//                BigDecimal.valueOf(3L).divide(sumCModel,
//                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
//            )
//        ), MathUtils.MATH_CONTEXT).add(oneDivTwo.multiply(BigDecimalMath.log(
//            oneDivTwo.divide(
//                BigDecimal.ONE.divide(sumCModel,
//                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
//            )
//        )), MathUtils.MATH_CONTEXT)
//            .divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);

    final BigDecimal result = MathUtils.klDivergence(dataSet);

    Assert.assertEquals("Score value differs", expected, result);
  }

  @Test
  public void testSumAndCalc_zeroValue_qModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ZERO, BigDecimal.ONE),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    }; // sum: q:3d, c:14d
    // r += (qModel/sums[qModel]) * log2((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final BigDecimal oneDivTwo = BigDecimal.ONE.divide(
        BigDecimal.valueOf(2L), MathUtils.MATH_CONTEXT);
    final BigDecimal sumCModel = BigDecimal.valueOf(4L);

    final BigDecimal expected =
        oneDivTwo.multiply(BigMathFunctions.ln(
            oneDivTwo.divide(
                BigDecimal.valueOf(3L).divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            ), MathUtils.MATH_CONTEXT.getPrecision()
        ), MathUtils.MATH_CONTEXT).add(oneDivTwo.multiply(BigMathFunctions.ln(
            oneDivTwo.divide(
                BigDecimal.ONE.divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            ), MathUtils.MATH_CONTEXT.getPrecision()
        )), MathUtils.MATH_CONTEXT)
            .divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);

//    final BigDecimal expected =
//        oneDivTwo.multiply(BigDecimalMath.log(
//            oneDivTwo.divide(
//                BigDecimal.valueOf(3L).divide(sumCModel,
//                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
//            )
//        ), MathUtils.MATH_CONTEXT).add(oneDivTwo.multiply(BigDecimalMath.log(
//            oneDivTwo.divide(
//                BigDecimal.ONE.divide(sumCModel,
//                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
//            )
//        )), MathUtils.MATH_CONTEXT)
//            .divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);

    final BigDecimal result = MathUtils.klDivergence(dataSet);

    Assert.assertEquals("Score value differs", expected, result);
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testSumAndCalc_nullElement()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        null,
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    };
    try {
      MathUtils.klDivergence(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testSumAndCalc_tooLowValue_cModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(-3L))
    };
    try {
      MathUtils.klDivergence(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testSumAndCalc_tooLowValue_qModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
        new ScoreTupleHighPrecision(BigDecimal.valueOf(-3L), BigDecimal.ONE)
    };
    try {
      MathUtils.klDivergence(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }
}