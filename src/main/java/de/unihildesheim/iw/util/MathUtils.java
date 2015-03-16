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

import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.GlobalConfiguration.DefaultKeys;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation
    .ScoreTupleHighPrecision;
import de.unihildesheim.iw.lucene.scoring.clarity.ClarityScoreCalculation
    .ScoreTupleLowPrecision;
import de.unihildesheim.iw.util.concurrent.AtomicBigDecimal;
import org.jetbrains.annotations.NotNull;
import org.nevec.rjm.BigDecimalMath;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;

/**
 * Collection of simple calculation utilities.
 */
public final class MathUtils {
  /**
   * Constant value of log2.
   */
  private static final double LOG2 = Math.log(2d);
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
   * Methods for calculating the Kullback-Leibler divergence.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class KlDivergenceHighPrecision {
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
            if (ds.qModel == null) {
              throw new IllegalArgumentException(
                  "Null as query-model value is not allowed.");
            }
            if (ds.cModel == null) {
              throw new IllegalArgumentException(
                  "Null as collection-model value is not allowed.");
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
            if (ds.qModel == null) {
              throw new IllegalArgumentException(
                  "Null as query-model value is not allowed.");
            }
            if (ds.cModel == null) {
              throw new IllegalArgumentException(
                  "Null as collection-model value are not allowed.");
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
                BigDecimalMath.log(
                    qScaled.divide(
                        ds.cModel.divide(sums.cModel, MATH_CONTEXT),
                        MATH_CONTEXT)), MATH_CONTEXT);
          })
          .forEach(s -> result.addAndGet(s, MATH_CONTEXT));

      return result.get().divide(BD_LOG2, MATH_CONTEXT);
    }
  }

  /**
   * Methods for calculating the Kullback-Leibler divergence.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class KlDivergenceLowPrecision {
    /**
     * Logger instance for this class.
     */
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
        KlDivergenceLowPrecision.class);

    /**
     * Runs summing and calculation of the KL-Divergence in one step.
     *
     * @param dataSet Data-set
     * @return KL-Divergence value
     */
    public static double sumAndCalc(
        @NotNull final ScoreTupleLowPrecision... dataSet) {
      return calc(dataSet, sumValues(dataSet));
    }

    /**
     * Sum values for a given data-set.
     *
     * @param dataSet Data-set to sum
     * @return Sums for values in the given data-set
     */
    static ScoreTupleLowPrecision sumValues(
        @NotNull final ScoreTupleLowPrecision... dataSet) {
      final double[] sums = Arrays.stream(dataSet)
          .filter(ds -> {
            // null values are not allowed
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
            // both values will be zero if cModel is zero
            // cModel == 0 implies qModel == 0
            // dividing zero is always zero, so skip if qModel == 0
            return ds.cModel > 0d && ds.qModel > 0d;
          })
          .map(ds -> new double[]{ds.qModel, ds.cModel})
          .reduce(new double[]{0d, 0d},
              (sum, curr) -> new double[]{sum[0] + curr[0], sum[1] + curr[1]});
      if (LOG.isDebugEnabled()) {
        LOG.debug("pqSum={} pcSum={}", sums[0], sums[1]);
      }
      return new ScoreTupleLowPrecision(sums[0], sums[1]);
    }

    /**
     * Calculate the KL-Divergence value.
     *
     * @param dataSet DataSet used for calculation
     * @param sums Sums of the {@code dataSet}
     * @return KL-Divergence value
     */
    static double calc(
        @NotNull final ScoreTupleLowPrecision[] dataSet,
        @NotNull final ScoreTupleLowPrecision sums) {
      return Arrays.stream(dataSet)
          .filter(ds -> {
            // null values are not allowed
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
            // ds.qModel will be zero if ds.cModel is zero:
            // ds.cModel == 0 implies ds.qModel == 0
            // dividing zero is always zero, so skip if qModel == 0
            return ds.cModel > 0d && ds.qModel > 0d;
          })
          .mapToDouble(ds -> {
            // scale value of ds.qModel & ds.cModel to [0,1]
            final double qScaled = ds.qModel / sums.qModel;
            // r += (ds.cModel/sums[cModel]) *
            // log((ds.cModel/sums[cModel]) / (ds.qModel/sums[qModel]))
            return qScaled * Math.log(qScaled / (ds.cModel / sums.cModel));
          }).sum();
    }
  }
}
