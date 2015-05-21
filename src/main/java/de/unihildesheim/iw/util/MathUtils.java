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
package de.unihildesheim.iw.util;

import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation.ScoreTupleHighPrecision;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation.ScoreTupleLowPrecision;
import de.unihildesheim.iw.util.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.util.concurrent.AtomicBigDecimal;
import de.unihildesheim.iw.util.concurrent.AtomicDouble;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;

/**
 * Collection of simple calculation utilities.
 */
public final class MathUtils {
  /**
   * Constant value of log10.
   */
  public static final double LOG10 = Math.log(10d);
  /**
   * Constant value of log2.
   */
  public static final double LOG2 = Math.log(2d);
  /**
   * Pre-calculated BigDecimal LOG2 value.
   */
  static final BigDecimal BD_LOG2 = BigDecimal.valueOf(Math.log(2d));
  /**
   * Math-context to use for high-precision calculations.
   */
  static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf()
          .getString(DefaultKeys.MATH_CONTEXT.toString(),
              GlobalConfiguration.DEFAULT_MATH_CONTEXT));

  /**
   * Private empty constructor for utility class.
   */
  private MathUtils() {
    // empty
  }

  /**
   * Calculate log2 for a given value.
   *
   * @param value Value to do the calculation for
   * @return Log2 of the given value
   */
  public static double log2(final double value) {
    return logN(2d, value);
  }

  /**
   * Calculate the log to a given base.
   *
   * @param base Base
   * @param value Value
   * @return Log of value to base
   */
  @SuppressWarnings("FloatingPointEquality")
  public static double logN(final double base, final double value) {
    if (base == 2d) {
      return Math.log(value) / LOG2;
    }
    if (base == 10d) {
      return Math.log10(value);
    }
    return Math.log(value) / Math.log(base);
  }

  /**
   * Scale values from a given domain to a given range.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Rescale {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(Rescale.class);
    /**
     * Range values.
     */
    final double[] values;
    final double divider;

    /**
     * Initializes the rescaling for the given domain and range.
     *
     * @param dom1 Domain lower bound
     * @param dom2 Domain upper bound
     * @param rng1 Range lower bound
     * @param rng2 Range upper bound
     */
    public Rescale(
        final double dom1, final double dom2,
        final double rng1, final double rng2) {
      this.values = new double[]{dom1, dom2, rng1, rng2};
      if (LOG.isDebugEnabled()) {
        LOG.debug("Scale dom={}~{} rng={}~{}", dom1, dom2, rng1, rng2);
      }

      this.divider = (this.values[1] - this.values[0]) == 0 ?
          1d / this.values[1] : this.values[1] - this.values[0];
    }

    /**
     * Initializes the rescaling for the given domain and the default range
     * between [>0, <1].
     *
     * @param dom1 Domain lower bound
     * @param dom2 Domain upper bound
     */
    public Rescale(
        final double dom1, final double dom2) {
      this(dom1, dom2, 0.001, 0.999);
    }

    /**
     * Scale the given value in the given domain defined at construction time to
     * the target range set at construction time. Domain bounds are not
     * checked!
     *
     * @param value Value to scale. Must be in bounds defined at construction
     * time.
     * @return Value scaled to target range
     */
    public double scale(final double value) {
      final double preScaled = (value - this.values[0]) / this.divider;
      final double result = (this.values[2] * (1d - preScaled)) +
          (this.values[3] * preScaled);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Scaling {} from {}~{} -> {}~{} = {}", value,
            this.values[0], this.values[1],
            this.values[2], this.values[3], result);
      }
      return result;
    }
  }

  /**
   * Methods for calculating the Kullback-Leibler divergence.
   */
  @SuppressWarnings("PublicInnerClass")
  private static final class KlDivergenceHighPrecision {
    /**
     * Logger instance for this class.
     */
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
        KlDivergenceHighPrecision.class);

    /**
     * Runs summing and calculation of the KL-Divergence in one step.
     *
     * @param dataSet Data-set
     * @return KL-Divergence value
     */
    public static BigDecimal sumAndCalc(
        @NotNull final ScoreTupleHighPrecision... dataSet) {
      return calc(dataSet, sumValues(dataSet));
    }

    /**
     * Sum values for a given data-set.
     *
     * @param dataSet Data-set to sum
     * @return Sums for values in the given data-set
     */
    public static ScoreTupleHighPrecision sumValues(
        @NotNull final ScoreTupleHighPrecision... dataSet) {
      final AtomicBigDecimal sumQModel = new AtomicBigDecimal();
      final AtomicBigDecimal sumCModel = new AtomicBigDecimal();

      Arrays.stream(dataSet)
          .filter(ds -> {
            // null values are not allowed
            if (ds == null) {
              throw new IllegalArgumentException(
                  "Null entries are not allowed in dataSet.");
            }
            final int qModelState = ds.qModel.compareTo(BigDecimal.ZERO);
            if (qModelState < 0) {
              throw new IllegalArgumentException(
                  "Values <0 as query-model value is not allowed.");
            }
            final int cModelState = ds.cModel.compareTo(BigDecimal.ZERO);
            if (cModelState < 0) {
              throw new IllegalArgumentException(
                  "Values <0 as collection-model value is not allowed.");
            }
            // both values will be zero if cModel is zero
            // cModel == 0 implies qModel == 0
            // dividing zero is always zero, so skip if qModel == 0
            return cModelState > 0 && qModelState > 0;
          })
          .forEach(ds -> {
                sumQModel.addAndGet(ds.qModel, MATH_CONTEXT);
                sumCModel.addAndGet(ds.cModel, MATH_CONTEXT);
              }
          );

      if (LOG.isDebugEnabled()) {
        LOG.debug("pcSum={} pqSum={}",
            sumQModel.doubleValue(), sumCModel.doubleValue());
      }
      return new ScoreTupleHighPrecision(sumQModel.get(), sumCModel.get());
    }

    /**
     * Calculate the KL-Divergence value.
     *
     * @param dataSet DataSet used for calculation
     * @param sums Sums of the {@code dataSet}
     * @return KL-Divergence value
     */
    static BigDecimal calc(
        @NotNull final ScoreTupleHighPrecision[] dataSet,
        @NotNull final ScoreTupleHighPrecision sums) {
      final AtomicBigDecimal result = new AtomicBigDecimal();

      Arrays.stream(dataSet)
          .filter(ds -> {
            // null values are not allowed
            if (ds == null) {
              throw new IllegalArgumentException(
                  "Null entries are not allowed in dataSet.");
            }
            final int qModelState = ds.qModel.compareTo(BigDecimal.ZERO);
            if (qModelState < 0) {
              throw new IllegalArgumentException(
                  "Values <0 as query-model value are not allowed.");
            }
            final int cModelState = ds.cModel.compareTo(BigDecimal.ZERO);
            if (cModelState < 0) {
              throw new IllegalArgumentException(
                  "Values <0 as collection-model value are not allowed.");
            }
            // both values will be zero if t2.b is zero
            // cModel == 0 implies qModel == 0
            return cModelState > 0 && qModelState > 0;
          })
          .map(ds -> {
            // scale value of qModel & cModel to [0,1]
            final BigDecimal qScaled =
                ds.qModel.divide(sums.qModel, MATH_CONTEXT);
            // r += (qModel/sums[qModel]) * log((qModel/sums[qModel]) /
            // (cModel /sums[cModel]))
            return qScaled.multiply(
                BigMathFunctions.ln(qScaled.divide(
                    ds.cModel.divide(sums.cModel, MATH_CONTEXT),
                    MATH_CONTEXT), MATH_CONTEXT.getPrecision()),
                MATH_CONTEXT);
//            return qScaled.multiply(
//                BigDecimalMath.log(
//                    qScaled.divide(
//                        ds.cModel.divide(sums.cModel, MATH_CONTEXT),
//                        MATH_CONTEXT)), MATH_CONTEXT);
          })
          .forEach(s -> result.addAndGet(s, MATH_CONTEXT));

      return result.get().divide(BD_LOG2, MATH_CONTEXT);
    }
  }

  public static BigDecimal klDivergence(
      @NotNull final ScoreTupleHighPrecision... dataSet) {
    final AtomicBigDecimal qModSum = new AtomicBigDecimal(0d);
    final AtomicBigDecimal cModSum = new AtomicBigDecimal(0d);

    // pre-check and estimate domain values
    Arrays.stream(dataSet)
        .forEach(ds -> {
          // check for illegal values in dataSet
          if (ds == null) {
            throw new IllegalArgumentException(
                "Null entries are not allowed in dataSet.");
          }
          final int cModelState = ds.cModel.compareTo(BigDecimal.ZERO);
          if (cModelState < 0) {
            throw new IllegalArgumentException(
                "Values <0 as collection-model value are not allowed.");
          }
          final int qModelState = ds.qModel.compareTo(BigDecimal.ZERO);
          if (qModelState < 0) {
            throw new IllegalArgumentException(
                "Values <0 as query-model value are not allowed.");
          }

          // sum values
          if (cModelState != 0 && qModelState != 0) { // check KL constraints
            qModSum.add(ds.qModel, MATH_CONTEXT);
            cModSum.add(ds.cModel, MATH_CONTEXT);
          }
        });

    // calculate result
    final BigDecimal qms = qModSum.get();
    final BigDecimal cms = cModSum.get();
    final AtomicBigDecimal result = new AtomicBigDecimal();

    assert qms.compareTo(BigDecimal.ZERO) != 0;
    assert cms.compareTo(BigDecimal.ZERO) != 0;

    Arrays.stream(dataSet)
        .filter(ds ->
            ds.cModel.compareTo(BigDecimal.ZERO) != 0 &&
                ds.qModel.compareTo(BigDecimal.ZERO) != 0)
        .map(ds -> {
          final BigDecimal qm = ds.qModel.divide(qms, MATH_CONTEXT);
          final BigDecimal cm = ds.cModel.divide(cms, MATH_CONTEXT);
          return qm.multiply(
              BigMathFunctions.ln(qm.divide(cm, MATH_CONTEXT),
                  MATH_CONTEXT.getPrecision()),
              MATH_CONTEXT
//              BigDecimalMath.log(
//                  qm.divide(cm, MATH_CONTEXT)
//              ), MATH_CONTEXT
          );
        }).forEach(s -> result.add(s, MATH_CONTEXT));
    return result.get().divide(BD_LOG2, MATH_CONTEXT);
  }

  public static double klDivergence(
      @NotNull final ScoreTupleLowPrecision... dataSet) {
    final AtomicDouble qModSum = new AtomicDouble(0d);
    final AtomicDouble cModSum = new AtomicDouble(0d);

    // pre-check and estimate domain values
    Arrays.stream(dataSet)
        .forEach(ds -> {
          // check for illegal values in dataSet
          if (ds == null) {
            throw new IllegalArgumentException(
                "Null entries are not allowed in dataSet.");
          }
          if (ds.cModel < 0d) {
            throw new IllegalArgumentException(
                "Values <0 as collection-model value are not allowed.");
          }
          if (ds.qModel < 0d) {
            throw new IllegalArgumentException(
                "Values <0 as query-model value are not allowed.");
          }

          // sum values
          if (ds.cModel != 0d && ds.qModel != 0d) { // check KL constraints
            qModSum.add(ds.qModel);
            cModSum.add(ds.cModel);
          }
        });

    // calculate result
    final double qms = qModSum.get();
    final double cms = cModSum.get();

    assert qms != 0d;
    assert cms != 0d;

    return Arrays.stream(dataSet)
        .filter(ds -> ds.cModel != 0d && ds.qModel != 0d)
        .mapToDouble(ds -> {
          final double qm = ds.qModel / qms;
          return qm * Math.log(qm / (ds.cModel / cms));
        }).sum() / LOG2;
  }
}
