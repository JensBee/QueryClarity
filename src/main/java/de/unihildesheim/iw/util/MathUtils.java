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
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.Tuple.Tuple2;
import de.unihildesheim.iw.util.concurrent.AtomicBigDecimal;
import org.nevec.rjm.BigDecimalMath;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.stream.StreamSupport;

/**
 * Collection of simple calculation utilities.
 */
public final class MathUtils {
  /**
   * Constant value of log2.
   */
  public static final double LOG2 = Math.log(2d);
  public static final BigDecimal BD_LOG2 = BigDecimal.valueOf(Math.log(2d));
  public static final MathContext MATH_CONTEXT = new MathContext(
      GlobalConfiguration.conf().getString(
          DefaultKeys.MATH_CONTEXT.toString()));
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
    /**
     * Static tuple value for a zero value result.
     */
    private static final Tuple2<BigDecimal, BigDecimal> ZERO_TUPLE =
        Tuple.tuple2(BigDecimal.ZERO, BigDecimal.ZERO);

    public static Tuple2<BigDecimal, BigDecimal> sumValues(
        final Iterable<Tuple2<BigDecimal, BigDecimal>> dataSet) {
      final AtomicBigDecimal sumA = new AtomicBigDecimal();
      final AtomicBigDecimal sumB = new AtomicBigDecimal();

      StreamSupport.stream(dataSet.spliterator(), true)
          .filter(t2 ->
              // a null value is not allowed here
              t2 != null && t2.a != null && t2.b != null &&
                  // both values will be zero if t2.b is zero
                  // t2.b == 0 implies t2.a == 0
                  t2.b.compareTo(BigDecimal.ZERO) != 0)
          .forEach(t -> {
                sumA.addAndGet(t.a, MATH_CONTEXT);
                sumB.addAndGet(t.b, MATH_CONTEXT);
              }
          );

      LOG.debug("pcSum={} pqSum={}",
          sumA.doubleValue(), sumB.doubleValue());
      return Tuple.tuple2(sumA.get(), sumB.get());
    }

    /*
    public static Tuple2<BigDecimal, BigDecimal> sumValues2(
        final Iterable<Tuple2<BigDecimal, BigDecimal>> dataSet)
        throws ProcessingException {
      final AtomicBigDecimal sumA = new AtomicBigDecimal();
      final AtomicBigDecimal sumB = new AtomicBigDecimal();

      new Processing().setSourceAndTarget(new TargetFuncCall<>(
          new IterableSource<>(dataSet),
          new TargetFunc<Tuple2<BigDecimal, BigDecimal>>() {

            @Override
            public void call(final Tuple2<BigDecimal, BigDecimal> term)
                throws DataProviderException {
              if (term == null) {
                return;
              }

              if (term.a == null || term.b == null) {
                LOG.warn("Null value in data-set. a={} b={}",
                    term.a == null ? "null" : term.a,
                    term.b == null ? "null" : term.b);
                return;
              }
              // t2.b == 0 implies t2.a == 0
              if (term.b.compareTo(BigDecimal.ZERO) == 0) {
                LOG.warn("data.b == 0, assuming data.a == 0 (implied)");
                return;
              }

              sumA.addAndGet(term.a, MATH_CONTEXT);
              sumB.addAndGet(term.b, MATH_CONTEXT);
            }
          }
      )).process();

      LOG.debug("pcSum={} pqSum={}",
          sumA.doubleValue(), sumB.doubleValue());
      return Tuple.tuple2(sumA.get(), sumB.get());
    }
    */

    public static BigDecimal calc(
        final Iterable<Tuple2<BigDecimal, BigDecimal>> values,
        final Tuple2<BigDecimal, BigDecimal> sums) {

      final AtomicBigDecimal result = new AtomicBigDecimal();

      StreamSupport.stream(values.spliterator(), true)
          .filter(t2 ->
              // a null value is not allowed here
              t2 != null && t2.a != null && t2.b != null &&
                  // t2.a will be zero if t2.b is zero:
                  // t2.b == 0 implies t2.a == 0
                  t2.b.compareTo(BigDecimal.ZERO) != 0 &&
                  // dividing zero is always zero, so skip here
                  t2.a.compareTo(BigDecimal.ZERO) != 0)
          .map(t2 -> {
            // scale value of t2.a & t2.b to [0,1]
            final BigDecimal aScaled = t2.a.divide(sums.a, MATH_CONTEXT);
            // r += (t2.a/sums.a) * log((t2.a/sums.a) / (t2.b/sums.b))

            return aScaled.multiply(
                BigDecimalMath.log(
                    aScaled.divide(
                        t2.b.divide(sums.b, MATH_CONTEXT),
                        MATH_CONTEXT)), MATH_CONTEXT);
          })
          .forEach(s -> result.addAndGet(s, MATH_CONTEXT));

      return result.get().divide(BD_LOG2, MATH_CONTEXT);
    }

    /*public static BigDecimal calc2(
        final Iterable<Tuple2<BigDecimal, BigDecimal>> values,
        final Tuple2<BigDecimal, BigDecimal> sums)
        throws ProcessingException {

      // cache values in a temporary db - holds all partial results
      // using MapDBs queue is much faster than using Java native ones
      final DB CACHE_DB = DBMakerUtils.newTempFileDB().make();
      final Queue<Object> rQueue = CACHE_DB
          .createQueue("resultCache", Serializer.BASIC, true);

      final Source<Tuple2<BigDecimal, BigDecimal>> source =
          new IterableSource<>(values);
      new Processing().setSourceAndTarget(new TargetFuncCall<>(
          source,
          new TargetFunc<Tuple2<BigDecimal, BigDecimal>>() {

            @Override
            public void call(final Tuple2<BigDecimal, BigDecimal> term) {
              if (term == null
                  || term.a == null || term.b == null
                  || term.a.compareTo(BigDecimal.ZERO) == 0
                  // data.b == 0 implies data.a == 0
                  || term.b.compareTo(BigDecimal.ZERO) == 0) {
                if (term == null) {
                  if (!source.isFinished()) {
                    LOG.warn("Skip data entry: NULL.");
                  }
                } else {
                  LOG.warn("Skip data entry: a={} b={}", term.a, term.b);
                }
                return;
              }

              // scale value of t2.a & t2.b to [0,1]
              final BigDecimal aScaled = term.a.divide(sums.a, MATH_CONTEXT);

              // r += (t2.a/sums.a) * log((t2.a/sums.a) / (t2.b/sums.b))
              rQueue.add(
                  aScaled.multiply(
                      BigDecimalMath.log(
                          aScaled.divide(
                              term.b.divide(sums.b, MATH_CONTEXT),
                              MATH_CONTEXT)), MATH_CONTEXT));
            }
          }
      )).process();

      // sum partial results
      BigDecimal result = BigDecimal.ZERO;
      BigDecimal value = (BigDecimal) rQueue.poll();
      while (value != null) {
        result = result.add(value, MATH_CONTEXT);
        value = (BigDecimal) rQueue.poll();
      }
      // remove temp db from disk
      CACHE_DB.close();
      return result.divide(BD_LOG2, MATH_CONTEXT);
    }*/
  }
}
