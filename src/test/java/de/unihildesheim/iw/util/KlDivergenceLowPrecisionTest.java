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
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation.ScoreTupleLowPrecision;
import de.unihildesheim.iw.util.MathUtils.KlDivergenceLowPrecision;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link KlDivergenceLowPrecision}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class KlDivergenceLowPrecisionTest
    extends TestCase {
  public KlDivergenceLowPrecisionTest() {
    super(LoggerFactory.getLogger(KlDivergenceLowPrecisionTest.class));
  }

  @Test
  public void testSumAndCalc()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 10d),
        new ScoreTupleLowPrecision(1d, 1d),
    }; // sum: q:3d, c:14d
    // r += (qModel/sums[qModel]) * log((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final double oneDivThree = 1d / 3d;
    final double expected =
        oneDivThree * Math.log(oneDivThree / (3d / 14d)) +
            oneDivThree * Math.log(oneDivThree / (10d / 14d)) +
            oneDivThree * Math.log(oneDivThree / (1d / 14d));
    final double result = KlDivergenceLowPrecision.sumAndCalc(dataSet);

    Assert.assertEquals("Score value differs", expected, result, 0d);
  }

  @Test
  public void testSumAndCalc_zeroValue_cModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 0d),
        new ScoreTupleLowPrecision(1d, 1d),
    }; // sum: q:2d, c:4d
    // r += (qModel/sums[qModel]) * log((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final double oneDivTwo = 1d / 2d;
    final double expected =
        oneDivTwo * Math.log(oneDivTwo / (3d / 4d)) +
            oneDivTwo * Math.log(oneDivTwo / (1d / 4d));
    final double result = KlDivergenceLowPrecision.sumAndCalc(dataSet);

    Assert.assertEquals("Score value differs", expected, result, 0d);
  }

  @Test
  public void testSumAndCalc_zeroValue_qModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(0d, 10d),
        new ScoreTupleLowPrecision(1d, 1d),
    }; // sum: q:2d, c:4d
    // r += (qModel/sums[qModel]) * log((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final double oneDivThree = 1d / 2d;
    final double expected =
        oneDivThree * Math.log(oneDivThree / (3d / 4d)) +
            oneDivThree * Math.log(oneDivThree / (1d / 4d));
    final double result = KlDivergenceLowPrecision.sumAndCalc(dataSet);

    Assert.assertEquals("Score value differs", expected, result, 0d);
  }

  @Test
  public void testSumAndCalc_nullElement()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        null,
        new ScoreTupleLowPrecision(1d, 1d),
    };
    try {
      KlDivergenceLowPrecision.sumAndCalc(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testSumAndCalc_tooLowValue_qModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 1d),
        new ScoreTupleLowPrecision(-1d, 3d),
        new ScoreTupleLowPrecision(1d, 3d),
    };
    try {
      KlDivergenceLowPrecision.sumAndCalc(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testSumAndCalc_tooLowValue_cModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 1d),
        new ScoreTupleLowPrecision(1d, -3d),
        new ScoreTupleLowPrecision(1d, 3d),
    };
    try {
      KlDivergenceLowPrecision.sumAndCalc(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testSumValues()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 10d),
        new ScoreTupleLowPrecision(1d, 1d),
    };
    // qModel, cModel
    final ScoreTupleLowPrecision expected = new ScoreTupleLowPrecision(3d, 14d);
    final ScoreTupleLowPrecision result =
        KlDivergenceLowPrecision.sumValues(dataSet);

    Assert.assertEquals("Summed qModel value differs",
        expected.qModel, result.qModel, 0d);
    Assert.assertEquals("Summed cModel value differs",
        expected.cModel, result.cModel, 0d);
  }

  @Test
  public void testSumValues_zeroValue_cModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 0d),
        new ScoreTupleLowPrecision(1d, 1d),
    };
    // qModel, cModel
    final ScoreTupleLowPrecision expected = new ScoreTupleLowPrecision(2d, 4d);
    final ScoreTupleLowPrecision result =
        KlDivergenceLowPrecision.sumValues(dataSet);

    Assert.assertEquals("Summed qModel value differs",
        expected.qModel, result.qModel, 0d);
    Assert.assertEquals("Summed cModel value differs",
        expected.cModel, result.cModel, 0d);
  }

  @Test
  public void testSumValues_zeroValue_qModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(0d, 10d),
        new ScoreTupleLowPrecision(1d, 1d),
    };
    // qModel, cModel
    final ScoreTupleLowPrecision expected = new ScoreTupleLowPrecision(2d, 4d);
    final ScoreTupleLowPrecision result =
        KlDivergenceLowPrecision.sumValues(dataSet);

    Assert.assertEquals("Summed qModel value differs",
        expected.qModel, result.qModel, 0d);
    Assert.assertEquals("Summed cModel value differs",
        expected.cModel, result.cModel, 0d);
  }

  @Test
  public void testSumValues_nullElement()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        null,
        new ScoreTupleLowPrecision(1d, 1d),
    };
    try {
      KlDivergenceLowPrecision.sumValues(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testSumValues_tooLowValue_qModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 1d),
        new ScoreTupleLowPrecision(-1d, 3d),
        new ScoreTupleLowPrecision(1d, 3d),
    };
    try {
      KlDivergenceLowPrecision.sumValues(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testSumValues_tooLowValue_cModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 1d),
        new ScoreTupleLowPrecision(1d, -3d),
        new ScoreTupleLowPrecision(1d, 3d),
    };
    try {
      KlDivergenceLowPrecision.sumValues(dataSet);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testCalc()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 10d),
        new ScoreTupleLowPrecision(1d, 1d),
    };
    final ScoreTupleLowPrecision sums = new ScoreTupleLowPrecision(3d, 14d);

    // r += (qModel/sums[qModel]) * log((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final double expected =
        (1d / sums.qModel) * Math.log((1d / sums.qModel) / (3d / sums.cModel)) +
            (1d / sums.qModel) *
                Math.log((1d / sums.qModel) / (10d / sums.cModel)) +
            (1d / sums.qModel) *
                Math.log((1d / sums.qModel) / (1d / sums.cModel));

    final double result = KlDivergenceLowPrecision.calc(dataSet, sums);
    Assert.assertEquals("Score value differs", expected, result, 0d);
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testCalc_null_set()
      throws Exception {
    final ScoreTupleLowPrecision sums = new ScoreTupleLowPrecision(3d, 14d);
    try {
      KlDivergenceLowPrecision.calc(null, sums);
      Assert.fail("Expected an NullPointerException to be thrown.");
    } catch (final NullPointerException e) {
      // pass
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testCalc_null_sums()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 10d),
        new ScoreTupleLowPrecision(1d, 1d),
    };
    try {
      KlDivergenceLowPrecision.calc(dataSet, null);
      Assert.fail("Expected an NullPointerException to be thrown.");
    } catch (final NullPointerException e) {
      // pass
    }
  }

  @Test
  public void testCalc_nullElement()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        null,
        new ScoreTupleLowPrecision(1d, 1d),
    };
    final ScoreTupleLowPrecision sums = new ScoreTupleLowPrecision(2d, 4d);
    try {
      KlDivergenceLowPrecision.calc(dataSet, sums);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testCalc_tooLowValue_qModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 1d),
        new ScoreTupleLowPrecision(-1d, 3d),
        new ScoreTupleLowPrecision(1d, 3d),
    };
    final ScoreTupleLowPrecision sums = new ScoreTupleLowPrecision(2d, 10d);
    try {
      KlDivergenceLowPrecision.calc(dataSet, sums);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testCalc_tooLowValue_cModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 1d),
        new ScoreTupleLowPrecision(1d, -3d),
        new ScoreTupleLowPrecision(1d, 3d),
    };
    final ScoreTupleLowPrecision sums = new ScoreTupleLowPrecision(4d, 4d);
    try {
      KlDivergenceLowPrecision.calc(dataSet, sums);
      Assert.fail("Expected an IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testCalc_zeroValue_cModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(1d, 0d),
        new ScoreTupleLowPrecision(1d, 1d),
    };
    final ScoreTupleLowPrecision sums = new ScoreTupleLowPrecision(2d, 4d);

    // r += (qModel/sums[qModel]) * log((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final double expected =
        (1d / sums.qModel) * Math.log((1d / sums.qModel) / (3d / sums.cModel)) +
            (1d / sums.qModel) *
                Math.log((1d / sums.qModel) / (1d / sums.cModel));

    final double result = KlDivergenceLowPrecision.calc(dataSet, sums);
    Assert.assertEquals("Score value differs", expected, result, 0d);
  }

  @Test
  public void testCalc_zeroValue_qModel()
      throws Exception {
    final ScoreTupleLowPrecision[] dataSet = {
        // qModel, cModel
        new ScoreTupleLowPrecision(1d, 3d),
        new ScoreTupleLowPrecision(0d, 10d),
        new ScoreTupleLowPrecision(1d, 1d),
    };
    final ScoreTupleLowPrecision sums = new ScoreTupleLowPrecision(2d, 4d);

    // r += (qModel/sums[qModel]) * log((qModel/sums[qModel]) /
    // (cModel/sums[cModel]))
    final double expected =
        (1d / sums.qModel) * Math.log((1d / sums.qModel) / (3d / sums.cModel)) +
            (1d / sums.qModel) *
                Math.log((1d / sums.qModel) / (1d / sums.cModel));

    final double result = KlDivergenceLowPrecision.calc(dataSet, sums);
    Assert.assertEquals("Score value differs", expected, result, 0d);
  }
}