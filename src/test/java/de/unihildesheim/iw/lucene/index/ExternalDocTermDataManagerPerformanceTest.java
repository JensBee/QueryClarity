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
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.TimeMeasure;
import org.junit.Test;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Test performance for different settings of {@link ExternalDocTermDataManager}
 * MapDB objects.
 *
 * @author Jens Bertram
 */
public final class ExternalDocTermDataManagerPerformanceTest
    extends PerformanceTestCase {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(ExternalDocTermDataManagerPerformanceTest.class);

  /**
   * Test map performance with different settings.
   */
  @Test
  public void testMap() {
    final TimeMeasure tm = new TimeMeasure();

    final BTreeKeySerializer mapKeySerializer
        = new BTreeKeySerializer.Tuple3KeySerializer<>(null, null,
        Serializer.STRING_INTERN, Serializer.INTEGER, ByteArray.SERIALIZER);

    for (final int nodeSize : NODE_SIZES) {
      for (final MapNodeType mnType : MapNodeType.values()) {
        for (int run = 0; run < RUNS; run++) {
          final DB db = getTempDB();

          final Map<Fun.Tuple3<String, Integer, ByteArray>, Object> extDataMap;
          switch (mnType) {
            case OUTSIDE:
              extDataMap = db.createTreeMap("test")
                  .valueSerializer(Serializer.BASIC)
                  .valuesOutsideNodesEnable()
                  .keySerializer(mapKeySerializer)
                  .make();
              break;
            default:
              extDataMap = db.createTreeMap("test")
                  .valueSerializer(Serializer.BASIC)
                  .keySerializer(mapKeySerializer)
                  .make();
              break;
          }


          tm.start();
          for (int i = 0; (double) i < 1e6; i++) {
            extDataMap.put(Fun.t3(
                RandomValue.getString(5),
                RandomValue.getInteger(),
                getTerm()
            ), RandomValue.getDouble());
          }

          LOG.info("Run={} NodeSize={} Type={} time={}",
              run, nodeSize, mnType, tm.stop().getElapsedSeconds());
          db.close();
        }
      }
    }
  }
}
