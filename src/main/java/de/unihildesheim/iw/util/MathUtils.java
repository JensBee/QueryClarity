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

import de.unihildesheim.iw.util.concurrent.AtomicBigDecimal;
import de.unihildesheim.iw.util.concurrent.processing.IterableSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.mapdb.Fun;
import org.nevec.rjm.BigDecimalMath;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;

/**
 * Collection of simple calculation utilities.
 */
public final class MathUtils {
  /**
   * Constant value of log2.
   */
  public static final double LOG2 = Math.log(2d);
  public static final BigDecimal BD_LOG2 = BigDecimal.valueOf(Math.log(2d));
  public static final MathContext MATH_CONTEXT = MathContext.DECIMAL128;
  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      MathUtils.class);

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
  public static final class KlDivergence {
    public static Fun.Tuple2<BigDecimal, BigDecimal> sumBigValues(
        final Iterable<Fun.Tuple2<BigDecimal, BigDecimal>> dataSet)
        throws ProcessingException {
      final AtomicBigDecimal sumA = new AtomicBigDecimal();
      final AtomicBigDecimal sumB = new AtomicBigDecimal();

      new Processing().setSourceAndTarget(new TargetFuncCall(
          new IterableSource<>(dataSet),
          new TargetFuncCall.TargetFunc<Fun.Tuple2<BigDecimal, BigDecimal>>() {

            @Override
            public void call(final Fun.Tuple2<BigDecimal, BigDecimal> data)
                throws Exception {
              if (data == null) {
                return;
              }

              if (data.a == null || data.b == null) {
                LOG.warn("Null value in data-set. a={} b={}",
                    data.a == null ? "null" : data.a,
                    data.b == null ? "null" : data.b);
                return;
              }
              // t2.b == 0 implies t2.a == 0
              if (data.b.compareTo(BigDecimal.ZERO) == 0) {
                return;
              }

              sumA.addAndGet(data.a);
              sumB.addAndGet(data.b);
            }
          }
      )).process();

      LOG.debug("pcSum={} pqSum={}",
          sumA.doubleValue(), sumB.doubleValue());
      return Fun.t2(sumA.get(), sumB.get());
    }

    /**
     * Sum up all values in the give {@code Tuple2} for all values in {@code
     * Tuple2 .a} (P) and {@code Tuple2.b} (Q). If {@code Tuple2.b} is zero
     * {@code Tuple2.a} is also assumed as being zero. (DKL(P||Q)).<br> Uses
     * {@link BigDecimal} for adding values together.
     *
     * @param dataSet Dataset to sum up with {@code Tuple2.a = P} and {@code
     * Tuple2.b = Q} (DKL(P||Q)).
     * @return Sums of all values in {@code Tuple2.a} and {@code Tuple2.b}
     * expressed in log space
     * @throws ProcessingException Thrown on errors encountered in parallel
     * processing of the values
     */
    public static Fun.Tuple2<BigDecimal, BigDecimal> sumValues(
        final Iterable<Fun.Tuple2<Double, Double>> dataSet)
        throws ProcessingException {
      final AtomicBigDecimal sumA = new AtomicBigDecimal();
      final AtomicBigDecimal sumB = new AtomicBigDecimal();

      new Processing().setSourceAndTarget(new TargetFuncCall(
          new IterableSource<>(dataSet),
          new TargetFuncCall.TargetFunc<Fun.Tuple2<Double, Double>>() {

            @Override
            public void call(final Fun.Tuple2<Double, Double> data)
                throws Exception {
              if (data == null) {
                return;
              }

              if (data.a == null || data.b == null) {
                LOG.warn("Null value in data-set. a={} b={}",
                    data.a == null ? "null" : data.a,
                    data.b == null ? "null" : data.b);
                return;
              }
              if (data.b == 0) { // t2.b == 0 implies t2.a == 0
                return;
              }

              sumA.addAndGet(BigDecimalCache.get(data.a));
              sumB.addAndGet(BigDecimalCache.get(data.b));
            }
          }
      )).process();

      LOG.debug("pcSum={} pqSum={}",
          sumA.doubleValue(), sumB.doubleValue());
      return Fun.t2(sumA.get(), sumB.get());
    }

    public static BigDecimal calcBig(
        final Iterable<Fun.Tuple2<BigDecimal, BigDecimal>> values,
        final Fun.Tuple2<BigDecimal, BigDecimal> sums)
        throws ProcessingException {

      final AtomicBigDecimal result = new AtomicBigDecimal();

      new Processing().setSourceAndTarget(new TargetFuncCall(
          new IterableSource<>(values),
          new TargetFuncCall.TargetFunc<Fun.Tuple2<BigDecimal, BigDecimal>>() {

            @Override
            public void call(final Fun.Tuple2<BigDecimal, BigDecimal> data)
                throws Exception {
              if (data == null) {
                return;
              }
              if (data.a == null || data.b == null) {
                LOG.warn("Null value in data-set. a={} b={}", data.a, data.b);
                return;
              }
              // data.b == 0 implies data.a == 0
              if (data.a.compareTo(BigDecimal.ZERO) == 0
                  || data.b.compareTo(BigDecimal.ZERO) == 0) {
                return;
              }

              // scale value of t2.a & t2.b to [0,1]
              final BigDecimal aScaled = data.a.divide(sums.a, MATH_CONTEXT);

              // r += (t2.a/sums.a) * log((t2.a/sums.a) / (t2.b/sums.b))
              result.addAndGet(
                  aScaled.multiply(
                      BigDecimalMath.log(aScaled)
                          .subtract(
                              BigDecimalMath.log(data.b
                                  .divide(sums.b, MATH_CONTEXT)))));

//              result.addAndGet(
//                  aScaled.multiply(
//                      BigDecimalCache.get(Math.log(aScaled.doubleValue()))
//                          .subtract(BigDecimalCache.get(Math.log(
//                                  data.b.divide(sums.b, MATH_CONTEXT)
//                                      .doubleValue())))));
            }
          }
      )).process();
      return result.get().divide(BD_LOG2, MATH_CONTEXT);
    }

    /**
     * Calculates the Kullback-Leibler divergence for a set of Double values.
     * The double values are expected as not being normalized. Normalization is
     * done by dividing each value in {@code Tuple.a} and {@code Tuple.b} by
     * {@code aSum} and {@code bSum}.
     *
     * @param values Plain, not normalized values with {@code Tuple2.a = P} and
     * {@code Tuple2.b = Q} (DKL(P||Q)).
     * @param sums Sums of all values in {@code Tuple2.a} and {@code Tuple2.b}
     * @return KL-Divergence
     */
    public static BigDecimal calc(
        final Iterable<Fun.Tuple2<Double, Double>> values,
        final Fun.Tuple2<BigDecimal, BigDecimal> sums)
        throws ProcessingException {

      final AtomicBigDecimal result = new AtomicBigDecimal();

      new Processing().setSourceAndTarget(new TargetFuncCall(
          new IterableSource<>(values),
          new TargetFuncCall.TargetFunc<Fun.Tuple2<Double, Double>>() {

            @Override
            public void call(final Fun.Tuple2<Double, Double> data)
                throws Exception {
              if (data == null) {
                return;
              }
              if (data.a == null || data.b == null) {
                LOG.warn("Null value in data-set. a={} b={}", data.a, data.b);
                return;
              }
              // data.b == 0 implies data.a == 0
              if (data.a == 0d || data.b == 0d) {
                return;
              }

              // scale value of t2.a & t2.b to [0,1]
              final BigDecimal aScaled = BigDecimalCache.get(data.a)
                  .divide(sums.a, MATH_CONTEXT);
              final BigDecimal bScaled = BigDecimalCache.get(data.b)
                  .divide(sums.b, MATH_CONTEXT);

              // r += (t2.a/sums.a) * log((t2.a/sums.a) / (t2.b/sums.b))
              result.addAndGet(
                  aScaled.multiply(
                      BigDecimalMath.log(aScaled)
                          .subtract(
                              BigDecimalMath.log(
                                  BigDecimalCache.get(data.b)
                                      .divide(sums.b, MATH_CONTEXT)))));
//              result.addAndGet(
//                  aScaled.multiply(
//                      BigDecimalCache.get(Math.log(aScaled.doubleValue()))
//                          .subtract(
//                              BigDecimalCache.get(
//                                  Math.log(bScaled.doubleValue()))
//                          )
//                  )
//              );
            }
          }
      )).process();
      return result.get().divide(BD_LOG2, MATH_CONTEXT);
    }
  }
}
