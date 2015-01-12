/*
 * Copyright (C) 2014 bhoerdzn
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

import de.unihildesheim.iw.mapdb.DBMakerUtils;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.junit.Test;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.nevec.rjm.BigDecimalMath;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MathUtilsPerformanceTest {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      MathUtilsPerformanceTest.class);

  @Test
  public void logCalcTest() {
    final BigDecimal val1 = BigDecimal.valueOf(
        RandomValue.getDouble(Double.MIN_VALUE, 1d));
    final BigDecimal val2 = BigDecimal.valueOf(
        RandomValue.getDouble(Double.MIN_VALUE, 1d));

    final TimeMeasure tm = new TimeMeasure();
    LOG.info("Two-log calc");
    tm.start();
    final BigDecimal res1 = BigDecimalMath.log(val1).subtract(BigDecimalMath
        .log(val2)).divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);
    LOG.info("Two-log: {} = {}",tm.stop().getElapsedMillis(), res1);
    LOG.info("Single-log calc");
    tm.start();
    final BigDecimal res2 = BigDecimalMath.log(val1.divide(val2,
        MathUtils.MATH_CONTEXT)).divide(MathUtils.BD_LOG2,
        MathUtils.MATH_CONTEXT);
    LOG.info("Single-log: {} = {}",tm.stop().getElapsedMillis(), res2);
  }

  @Test
  public void calcBigTest()
      throws ProcessingException {
    final Collection<Fun.Tuple2<BigDecimal, BigDecimal>> data = new ArrayList();
    final int dataSize = 3000000;
    LOG.info("Generating random dataset. size={}", dataSize);
    for (int i = 0; i < dataSize; i++) {
      data.add(Fun.t2(
          BigDecimal.valueOf(
              RandomValue.getDouble(Double.MIN_VALUE, Double.MAX_VALUE)),
          BigDecimal.valueOf(
              RandomValue.getDouble(Double.MIN_VALUE, Double.MAX_VALUE))
      ));
    }

    final TimeMeasure tm = new TimeMeasure();
    LOG.info("Calculating sums");
    final Fun.Tuple2<BigDecimal, BigDecimal> sums =
        MathUtils.KlDivergence.sumValues(data);
    LOG.info("Sums: {}, {}", sums.a, sums.b);
    LOG.info("Calculating..");
    tm.start();
    MathUtils.KlDivergence.calc(data, sums);
    LOG.info("Implementation: {}", tm.stop().getTimeString());

    LOG.info("Calculating reference..");
    tm.start();
    refCalcBig(data, sums);
    LOG.info("Reference: {}", tm.stop().getTimeString());
  }

  private static BigDecimal refCalcBig(
      final Iterable<Fun.Tuple2<BigDecimal, BigDecimal>> values,
      final Fun.Tuple2<BigDecimal, BigDecimal> sums)
      throws ProcessingException {

    // gather duplicate entries to speedup calculation
    final DB deDupeDb = DBMakerUtils.newTempFileDB().make();
    final Map<Fun.Tuple2<BigDecimal, BigDecimal>, Long> valMap = deDupeDb
        .createTreeMap("deDupe")
        .keySerializer(new BTreeKeySerializer.Tuple2KeySerializer<>(
            null, Serializer.BASIC, Serializer.BASIC))
        .valueSerializer(Serializer.LONG)
        .make();
    for (final Fun.Tuple2<BigDecimal, BigDecimal> value : values) {
      if (valMap.containsKey(value)) {
        valMap.put(value, valMap.get(value) + 1L);
      } else {
        valMap.put(value, 1L);
      }
    }

    // holds all partial results
    final Collection<BigDecimal> resultQueue = new ConcurrentLinkedQueue<>();

    new Processing().setSourceAndTarget(new TargetFuncCall(
        new CollectionSource(valMap.keySet()),
        new TargetFuncCall.TargetFunc<Fun.Tuple2<BigDecimal, BigDecimal>>() {

          @Override
          public void call(final Fun.Tuple2<BigDecimal, BigDecimal> data)
              throws Exception {
            if (data == null
                || data.a == null || data.b == null
                || data.a.compareTo(BigDecimal.ZERO) == 0
                // data.b == 0 implies data.a == 0
                || data.b.compareTo(BigDecimal.ZERO) == 0) {
              return;
            }

            // scale value of t2.a & t2.b to [0,1]
            final BigDecimal aScaled = data.a.divide(sums.a,
                MathUtils.MATH_CONTEXT);
            final BigDecimal bScaled =
                data.b.divide(sums.b, MathUtils.MATH_CONTEXT);

            final BigDecimal log = BigDecimalMath.log(
                aScaled.divide(bScaled, MathUtils.MATH_CONTEXT)
            );

            // r += (t2.a/sums.a) * log((t2.a/sums.a) / (t2.b/sums.b))
            if (valMap.get(data) > 1L) {
              final long times = valMap.get(data);
              final BigDecimal value = aScaled.multiply(log);
              for (long i = 0; i < times; i++) {
                resultQueue.add(value);
              }
            } else {
              resultQueue.add(aScaled.multiply(log));
            }

//              resultQueue.add(
//                  aScaled.multiply(
//                      BigDecimalMath.log(
//                          aScaled.divide(
//                              data.b.divide(sums.b, MATH_CONTEXT),
// MATH_CONTEXT)
//                      )
//                  )
//              );
//              resultQueue.add(
//                  aScaled.multiply(
//                      BigDecimalMath.log(aScaled)
//                          .subtract(
//                              BigDecimalMath.log(data.b
//                                  .divide(sums.b, MATH_CONTEXT)))));
          }
        }
    )).process();

    // sum partial results
    BigDecimal result = BigDecimal.ZERO;
    for (final BigDecimal part : resultQueue) {
      result = result.add(part);
    }
    return result.divide(MathUtils.BD_LOG2, MathUtils.MATH_CONTEXT);
  }

}