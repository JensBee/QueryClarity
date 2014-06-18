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

package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.PerformanceTestCase;
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.TimeMeasure;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Test performance for different settings of {@link DirectIndexDataProvider}
 * MapDB objects. For maps it's assumed that {@code valuesOutsideNodesEnable ()}
 * is not set while testing.
 *
 * @author Jens Bertram
 */
public final class DirectIndexDataProviderPerformanceTest
    extends PerformanceTestCase {

  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(DirectIndexDataProviderPerformanceTest.class);

  /**
   * Test settings for index terms Set.
   *
   * @see DirectIndexDataProvider.CacheDbMakers#idxTermsMaker(DB)
   */
  @Test
  public void testIdxTermsSet() {
    final TimeMeasure tm = new TimeMeasure();

    for (final int nodeSize : NODE_SIZES) {
      for (int run = 0; run < RUNS; run++) {
        final DB db = getTempDB();
        final Set<ByteArray> idxTermsSet =
            DirectIndexDataProvider.CacheDbMakers
                .idxTermsMaker(db).nodeSize(nodeSize).make();

        tm.start();
        for (int i = 0; (double) i < 1e6; i++) {
          idxTermsSet.add(getTerm());
        }

        LOG.info("Run={} NodeSize={} time={}",
            run, nodeSize, tm.stop().getElapsedSeconds());
        db.close();
      }
    }
  }

  /**
   * Test settings for document-frequency map.
   *
   * @see DirectIndexDataProvider.CacheDbMakers#idxDfMapMaker(DB)
   */
  @Test
  public void testIdxDfMap() {
    final TimeMeasure tm = new TimeMeasure();

    for (final int nodeSize : NODE_SIZES) {
      for (final MapNodeType mnType : MapNodeType.values()) {
        for (int run = 0; run < RUNS; run++) {
          final DB db = getTempDB();

          final Map<ByteArray, Integer> idxDfMap;
          switch (mnType) {
            case OUTSIDE:
              idxDfMap = DirectIndexDataProvider.CacheDbMakers
                  .idxDfMapMaker(db).nodeSize(nodeSize)
                  .valuesOutsideNodesEnable().make();
              break;
            default:
              idxDfMap = DirectIndexDataProvider.CacheDbMakers
                  .idxDfMapMaker(db).nodeSize(nodeSize).make();
              break;
          }


          tm.start();
          for (int i = 0; (double) i < 1e6; i++) {
            idxDfMap.put(getTerm(), RandomValue.getInteger());
          }

          LOG.info("Run={} NodeSize={} Type={} time={}",
              run, nodeSize, mnType, tm.stop().getElapsedSeconds());
          db.close();
        }
      }
    }
  }

  /**
   * Test settings for document-terms map.
   *
   * @see DirectIndexDataProvider.CacheDbMakers#idxDocTermsMapMkr(DB)
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Test
  public void testIdxDocTermsMap() {
    final TimeMeasure tm = new TimeMeasure();

    for (final int nodeSize : NODE_SIZES) {
      for (final MapNodeType mnType : MapNodeType.values()) {
        for (int run = 0; run < RUNS; run++) {
          final DB db = getTempDB();

          final Map<Fun.Tuple3<
              ByteArray, SerializableByte, Integer>, Integer> idxDocTermsMap;
          switch (mnType) {
            case OUTSIDE:
              idxDocTermsMap = DirectIndexDataProvider.CacheDbMakers
                  .idxDocTermsMapMkr(db).nodeSize(nodeSize)
                  .valuesOutsideNodesEnable().make();
              break;
            default:
              idxDocTermsMap = DirectIndexDataProvider.CacheDbMakers
                  .idxDocTermsMapMkr(db).nodeSize(nodeSize).make();
              break;
          }


          tm.start();
          for (int i = 0; (double) i < 1e6; i++) {
            idxDocTermsMap.put(
                Fun.t3(
                    getTerm(),
                    new SerializableByte(RandomValue.getByte()),
                    RandomValue.getInteger()),
                RandomValue.getInteger()
            );
          }

          LOG.info("Run={} NodeSize={} Type={} time={}",
              run, nodeSize, mnType, tm.stop().getElapsedSeconds());
          db.close();
        }
      }
    }
  }

  /**
   * Test settings for index terms Map.
   *
   * @see DirectIndexDataProvider.CacheDbMakers#idxTermsMapMkr(DB)
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Test
  public void testIdxTermsMap() {
    final TimeMeasure tm = new TimeMeasure();

    for (final int nodeSize : NODE_SIZES) {
      for (final MapNodeType mnType : MapNodeType.values()) {
        for (int run = 0; run < RUNS; run++) {
          final DB db = getTempDB();

          final Map<Fun.Tuple2<SerializableByte, ByteArray>, Long> idxTermsMap;
          switch (mnType) {
            case OUTSIDE:
              idxTermsMap = DirectIndexDataProvider.CacheDbMakers
                  .idxTermsMapMkr(db).nodeSize(nodeSize)
                  .valuesOutsideNodesEnable().make();
              break;
            default:
              idxTermsMap = DirectIndexDataProvider.CacheDbMakers
                  .idxTermsMapMkr(db).nodeSize(nodeSize).make();
              break;
          }


          tm.start();
          for (int i = 0; (double) i < 1e6; i++) {
            idxTermsMap.put(
                Fun.t2(
                    new SerializableByte(RandomValue.getByte()),
                    getTerm()),
                RandomValue.getLong()
            );
          }

          LOG.info("Run={} NodeSize={} Type={} time={}",
              run, nodeSize, mnType, tm.stop().getElapsedSeconds());
          db.close();
        }
      }
    }
  }

  /**
   * Test settings for cached fields Map.
   *
   * @see DirectIndexDataProvider.CacheDbMakers#cachedFieldsMapMaker(DB)
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Test
  public void testCachedFieldsMap() {
    final TimeMeasure tm = new TimeMeasure();

    for (final int nodeSize : NODE_SIZES) {
      for (final MapNodeType mnType : MapNodeType.values()) {
        for (int run = 0; run < RUNS; run++) {
          final DB db = getTempDB();

          final Map<String, SerializableByte> cachedFieldsMap;
          switch (mnType) {
            case OUTSIDE:
              cachedFieldsMap = DirectIndexDataProvider.CacheDbMakers
                  .cachedFieldsMapMaker(db).nodeSize(nodeSize)
                  .valuesOutsideNodesEnable().make();
              break;
            default:
              cachedFieldsMap = DirectIndexDataProvider.CacheDbMakers
                  .cachedFieldsMapMaker(db).nodeSize(nodeSize).make();
              break;
          }


          tm.start();
          for (int i = 0; (double) i < 1e6; i++) {
            cachedFieldsMap.put(
                RandomValue.getString(2, 15),
                new SerializableByte(RandomValue.getByte()));
          }

          LOG.info("Run={} NodeSize={} Type={} time={}",
              run, nodeSize, mnType, tm.stop().getElapsedSeconds());
          db.close();
        }
      }
    }
  }
}
