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
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation
    .ScoreTupleHighPrecision;
import de.unihildesheim.iw.util.MathUtils.KlDivergenceHighPrecision;
import org.junit.Assert;
import org.junit.Test;
import org.nevec.rjm.BigDecimalMath;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Test for {@link KlDivergenceHighPrecision}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class KlDivergenceHighPrecisionTest
    extends TestCase {
  public KlDivergenceHighPrecisionTest() {
    super(LoggerFactory.getLogger(KlDivergenceHighPrecisionTest.class));
  }

  @Test
  public void testSumAndCalc()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(10L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    }; // sum: q:3d, c:14d
    // r += (qModel/sums[qModel]) * log((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final BigDecimal oneDivThree = BigDecimal.ONE.divide(
        BigDecimal.valueOf(3L), MathUtils.MATH_CONTEXT);
    final BigDecimal sumCModel = BigDecimal.valueOf(14L);

    final BigDecimal expected =
        oneDivThree.multiply(BigDecimalMath.log(
            oneDivThree.divide(
                BigDecimal.valueOf(3L).divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            )
        ), MathUtils.MATH_CONTEXT).add(oneDivThree.multiply(BigDecimalMath.log(
            oneDivThree.divide(
                BigDecimal.valueOf(10L).divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            )
        )), MathUtils.MATH_CONTEXT).add(oneDivThree.multiply(BigDecimalMath.log(
            oneDivThree.divide(
                BigDecimal.ONE.divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            )
        )), MathUtils.MATH_CONTEXT)
            .divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);

    final BigDecimal result = KlDivergenceHighPrecision.sumAndCalc(dataSet);

    Assert.assertEquals("Score value differs", expected, result);
  }

  @Test
  public void testSumAndCalc_zeroValue_cModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ZERO),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    }; // sum: q:3d, c:14d
    // r += (qModel/sums[qModel]) * log((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final BigDecimal oneDivTwo = BigDecimal.ONE.divide(
        BigDecimal.valueOf(2L), MathUtils.MATH_CONTEXT);
    final BigDecimal sumCModel = BigDecimal.valueOf(4L);

    final BigDecimal expected =
        oneDivTwo.multiply(BigDecimalMath.log(
            oneDivTwo.divide(
                BigDecimal.valueOf(3L).divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            )
        ), MathUtils.MATH_CONTEXT).add(oneDivTwo.multiply(BigDecimalMath.log(
            oneDivTwo.divide(
                BigDecimal.ONE.divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            )
        )), MathUtils.MATH_CONTEXT)
            .divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);

    final BigDecimal result = KlDivergenceHighPrecision.sumAndCalc(dataSet);

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
    // r += (qModel/sums[qModel]) * log((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final BigDecimal oneDivTwo = BigDecimal.ONE.divide(
        BigDecimal.valueOf(2L), MathUtils.MATH_CONTEXT);
    final BigDecimal sumCModel = BigDecimal.valueOf(4L);

    final BigDecimal expected =
        oneDivTwo.multiply(BigDecimalMath.log(
            oneDivTwo.divide(
                BigDecimal.valueOf(3L).divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            )
        ), MathUtils.MATH_CONTEXT).add(oneDivTwo.multiply(BigDecimalMath.log(
            oneDivTwo.divide(
                BigDecimal.ONE.divide(sumCModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT
            )
        )), MathUtils.MATH_CONTEXT)
            .divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);

    final BigDecimal result = KlDivergenceHighPrecision.sumAndCalc(dataSet);

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
      KlDivergenceHighPrecision.sumAndCalc(dataSet);
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
      KlDivergenceHighPrecision.sumAndCalc(dataSet);
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
      KlDivergenceHighPrecision.sumAndCalc(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testSumValues()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(10L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    }; // sum: q:3d, c:14d
    // qModel, cModel
    final ScoreTupleHighPrecision
        expected = new ScoreTupleHighPrecision(
        BigDecimal.valueOf(3L), BigDecimal.valueOf(14L));
    final ScoreTupleHighPrecision result = KlDivergenceHighPrecision
        .sumValues(dataSet);

    Assert.assertEquals("Summed qModel value differs",
        expected.qModel, result.qModel);
    Assert.assertEquals("Summed cModel value differs",
        expected.cModel, result.cModel);
  }

  @Test
  public void testSumValues_nullElement()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        null,
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    };
    try {
      KlDivergenceHighPrecision.sumValues(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testSumValues_tooLowValue_cModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(-3L))
    };
    try {
      KlDivergenceHighPrecision.sumValues(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testSumValues_tooLowValue_qModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
        new ScoreTupleHighPrecision(BigDecimal.valueOf(-3L), BigDecimal.ONE)
    };
    try {
      KlDivergenceHighPrecision.sumValues(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testCalc()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(10L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    }; // sum: q:3d, c:14d
    // qModel, cModel
    final ScoreTupleHighPrecision
        sums = new ScoreTupleHighPrecision(
        BigDecimal.valueOf(3L), BigDecimal.valueOf(14L));

    // r += (qModel/sums[qModel]) * log((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final BigDecimal oneDivSumQModel = BigDecimal.ONE.divide(
        sums.qModel, MathUtils.MATH_CONTEXT);
    final BigDecimal expected =
        oneDivSumQModel.multiply(BigDecimalMath.log(
                oneDivSumQModel.divide(
                    BigDecimal.valueOf(3L).divide(sums.cModel,
                        MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT)
            ), MathUtils.MATH_CONTEXT
        ).add(oneDivSumQModel.multiply(BigDecimalMath.log(
                oneDivSumQModel.divide(
                    BigDecimal.valueOf(10L).divide(sums.cModel,
                        MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT)
            ), MathUtils.MATH_CONTEXT
        )).add(oneDivSumQModel.multiply(BigDecimalMath.log(
            oneDivSumQModel.divide(
                BigDecimal.ONE.divide(sums.cModel,
                    MathUtils.MATH_CONTEXT), MathUtils.MATH_CONTEXT)
        ), MathUtils.MATH_CONTEXT))
            .divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);

    final BigDecimal result = KlDivergenceHighPrecision.calc(dataSet, sums);
    Assert.assertEquals("Score value differs", expected, result);
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testCalc_nullElement_set_qModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(null, BigDecimal.valueOf(10L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    };
    final ScoreTupleHighPrecision
        sums = new ScoreTupleHighPrecision(
        BigDecimal.valueOf(3L), BigDecimal.valueOf(14L));
    try {
      KlDivergenceHighPrecision.calc(dataSet, sums);
      Assert.fail("Expected a NullPointerException to be thrown.");
    } catch (final NullPointerException e) {
      // pass
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testCalc_nullElement_set_cModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, null),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    };
    final ScoreTupleHighPrecision
        sums = new ScoreTupleHighPrecision(
        BigDecimal.valueOf(3L), BigDecimal.valueOf(14L));
    try {
      KlDivergenceHighPrecision.calc(dataSet, sums);
      Assert.fail("Expected an NullPointerException to be thrown.");
    } catch (final NullPointerException e) {
      // pass
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testCalc_nullElement_sums_cModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    };
    final ScoreTupleHighPrecision
        sums = new ScoreTupleHighPrecision(BigDecimal.valueOf(3L), null);
    try {
      KlDivergenceHighPrecision.calc(dataSet, sums);
      Assert.fail("Expected an NullPointerException to be thrown.");
    } catch (final NullPointerException e) {
      // pass
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testCalc_nullElement_sums_qModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    };
    final ScoreTupleHighPrecision
        sums = new ScoreTupleHighPrecision(null, BigDecimal.valueOf(3L));
    try {
      KlDivergenceHighPrecision.calc(dataSet, sums);
      Assert.fail("Expected an NullPointerException to be thrown.");
    } catch (final NullPointerException e) {
      // pass
    }
  }

  @Test
  public void testSumValues_zeroValue_cModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ZERO),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    };
    final ScoreTupleHighPrecision expected = new ScoreTupleHighPrecision(
        BigDecimal.valueOf(2L), BigDecimal.valueOf(4L));
    final ScoreTupleHighPrecision result = KlDivergenceHighPrecision
        .sumValues(dataSet);

    Assert.assertEquals("Summed qModel value differs",
        expected.qModel, result.qModel);
    Assert.assertEquals("Summed cModel value differs",
        expected.cModel, result.cModel);
  }

  @Test
  public void testSumValues_zeroValue_qModel()
      throws Exception {
    final ScoreTupleHighPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.valueOf(3L)),
        new ScoreTupleHighPrecision(BigDecimal.ZERO, BigDecimal.ONE),
        new ScoreTupleHighPrecision(BigDecimal.ONE, BigDecimal.ONE),
    };
    final ScoreTupleHighPrecision expected = new ScoreTupleHighPrecision(
        BigDecimal.valueOf(2L), BigDecimal.valueOf(4L));
    final ScoreTupleHighPrecision result = KlDivergenceHighPrecision
        .sumValues(dataSet);

    Assert.assertEquals("Summed qModel value differs",
        expected.qModel, result.qModel);
    Assert.assertEquals("Summed cModel value differs",
        expected.cModel, result.cModel);
  }
}